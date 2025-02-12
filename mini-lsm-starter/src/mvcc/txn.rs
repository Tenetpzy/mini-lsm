// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::{BTreeMap, HashSet},
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, ensure, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

struct ReadSet {
    // 记录事务中的单个get，加快比较，只记录hash值，但导致可能误判
    read_key_hashes: HashSet<u32>,

    // 记录事务读的谓词，用于实现真正的SSI
    // 可以用区间树优化
    read_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
}

impl ReadSet {
    fn new() -> Self {
        Self {
            read_key_hashes: HashSet::new(),
            read_ranges: Vec::new(),
        }
    }

    fn hash_key(key: &[u8]) -> u32 {
        farmhash::hash32(key)
    }

    fn add_key(&mut self, key: &[u8]) {
        self.read_key_hashes.insert(Self::hash_key(key));
    }

    fn add_range(&mut self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) {
        self.read_ranges.push((
            lower.map(Bytes::copy_from_slice),
            upper.map(Bytes::copy_from_slice),
        ));
    }

    fn is_intersect(&self, write_set: &WriteSet) -> bool {
        for key in write_set.iter() {
            if self.read_key_hashes.contains(&Self::hash_key(key)) {
                return true;
            }

            for (lower, upper) in &self.read_ranges {
                let greater_lower = match lower {
                    Bound::Excluded(k) => key > k,
                    Bound::Included(k) => key >= k,
                    Bound::Unbounded => true,
                };
                let less_upper = match upper {
                    Bound::Excluded(k) => key < k,
                    Bound::Included(k) => key <= k,
                    Bound::Unbounded => true,
                };
                if greater_lower && less_upper {
                    return true;
                }
            }
        }

        false
    }
}

#[derive(Default)]
pub struct WriteSet {
    write_keys: HashSet<Bytes>,
}

impl WriteSet {
    fn new() -> Self {
        Self {
            write_keys: HashSet::new(),
        }
    }

    fn iter(&self) -> std::collections::hash_set::Iter<'_, Bytes> {
        self.write_keys.iter()
    }

    fn add_key(&mut self, key: &[u8]) {
        self.write_keys.insert(Bytes::copy_from_slice(key));
    }

    fn is_empty(&self) -> bool {
        self.write_keys.is_empty()
    }
}

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: AtomicBool,
    /// 如果创建事务时serializable为true，则会记录事务读写集合
    rw_set: Option<Mutex<(ReadSet, WriteSet)>>,
}

impl Transaction {
    pub(crate) fn new(inner: Arc<LsmStorageInner>, read_ts: u64, serializable: bool) -> Self {
        inner.mvcc().watermark_add_ts(read_ts);
        Self {
            read_ts,
            inner,
            local_storage: Arc::new(SkipMap::new()),
            committed: AtomicBool::new(false),
            rw_set: if serializable {
                Some(Mutex::new((ReadSet::new(), WriteSet::new())))
            } else {
                None
            },
        }
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        if let Some(rwset) = &self.rw_set {
            rwset.lock().0.add_key(key);
        }

        let iter = self.scan_inner(Bound::Included(key), Bound::Included(key))?;
        if iter.is_valid() && iter.key() == key {
            Ok(Some(Bytes::copy_from_slice(iter.value())))
        } else {
            Ok(None)
        }
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        if let Some(rwset) = &self.rw_set {
            rwset.lock().0.add_range(lower, upper);
        }
        self.scan_inner(lower, upper)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        if let Some(rwset) = &self.rw_set {
            rwset.lock().1.add_key(key);
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        if let Some(rwset) = &self.rw_set {
            rwset.lock().1.add_key(key);
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    /// 提交失败（回滚或其它）则返回错误
    pub fn commit(&self) -> Result<()> {
        let mvcc = self.inner.mvcc();
        let _guard = mvcc.commit_lock.lock();

        ensure!(
            self.committed
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok(),
            "transaction has already committed!"
        );

        let mut batch: Vec<WriteBatchRecord<Bytes>> = Vec::new();
        for entry in self.local_storage.iter() {
            if entry.value().is_empty() {
                batch.push(WriteBatchRecord::Del(entry.key().clone()));
            } else {
                batch.push(WriteBatchRecord::Put(
                    entry.key().clone(),
                    entry.value().clone(),
                ));
            }
        }

        let mut committed_txns = mvcc.committed_txns().lock();

        if let Some(rwset) = &self.rw_set {
            if !self.validate_commit(&committed_txns) {
                bail!("Transaction(read ts = {}) rollback", self.read_ts);
            }

            let commit_ts = self.inner.direct_write_batch(&batch)?;

            // 增加已提交事务的信息
            let old = committed_txns.insert(commit_ts, std::mem::take(&mut rwset.lock().1));
            assert!(old.is_none());
        } else {
            // 事务不需要可串行化验证，直接提交
            self.inner.direct_write_batch(&batch)?;
        }

        // 将watermark及以下的事务从已提交事务信息中移除，它们不会再参与验证
        let watermark = mvcc.watermark();
        while let Some((&ts, _)) = committed_txns.first_key_value() {
            if ts <= watermark {
                committed_txns.remove(&ts);
            } else {
                break;
            }
        }

        Ok(())
    }

    /// 验证当前事务是否可以提交，如果当前事务开启了SSI，则进行验证
    /// 如果当前事务只读，可以直接提交，否则
    /// 向前验证，比较在本事务开始后，提交前，已经提交的那些事务的写集合，与当前事务的读集合是否有交集
    fn validate_commit(&self, committed_txns: &BTreeMap<u64, WriteSet>) -> bool {
        if let Some(rwset) = &self.rw_set {
            let rwset = rwset.lock();
            if rwset.1.is_empty() {
                true
            } else {
                for (_, txn_write_set) in committed_txns.range(self.read_ts + 1..) {
                    if rwset.0.is_intersect(txn_write_set) {
                        return false;
                    }
                }
                true
            }
        } else {
            true
        }
    }

    fn scan_inner(
        self: &Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut local_iter: TxnLocalIterator = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| {
                map.range((
                    lower.map(Bytes::copy_from_slice),
                    upper.map(Bytes::copy_from_slice),
                ))
            },
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_iter.next()?; // 设置初始状态

        TxnIterator::create(self.clone(), local_iter, lsm_iter)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().watermark_reduce_ts(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|item| item.1.as_ref())
    }

    fn key(&self) -> &[u8] {
        self.with_item(|item| &item.0)
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            *fields.item = match fields.iter.next() {
                Some(entry) => (entry.key().clone(), entry.value().clone()),
                None => (Bytes::new(), Bytes::new()),
            };
            Ok(())
        })
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        local_iter: TxnLocalIterator,
        lsm_iter: FusedIterator<LsmIterator>,
    ) -> Result<Self> {
        let mut iter = Self {
            txn,
            iter: TwoMergeIterator::create(local_iter, lsm_iter)?,
        };
        iter.skip_deleted_keys()?;
        Ok(iter)
    }

    fn skip_deleted_keys(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deleted_keys()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
