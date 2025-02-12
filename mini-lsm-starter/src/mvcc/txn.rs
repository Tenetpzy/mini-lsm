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
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: AtomicBool,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub(crate) fn new(inner: Arc<LsmStorageInner>, read_ts: u64, _serializable: bool) -> Self {
        inner.mvcc().watermark_add_ts(read_ts);
        Self {
            read_ts,
            inner,
            local_storage: Arc::new(SkipMap::new()),
            committed: AtomicBool::new(false),
            key_hashes: None,
        }
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        let iter = self.scan(Bound::Included(key), Bound::Included(key))?;
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

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::Relaxed) {
            panic!("can not operate on committed transaction");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        self.committed.store(true, Ordering::Relaxed);
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

        self.inner.write_batch(&batch)
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
