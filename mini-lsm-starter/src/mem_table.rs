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

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: AtomicUsize,
}

pub(crate) fn map_bound_keyslice(bound: Bound<&[u8]>, timestamp: u64) -> Bound<KeySlice> {
    bound.map(|key| KeySlice::from_slice(key, timestamp))
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: AtomicUsize::new(0),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut memtable = Self::create(id);
        let wal = Wal::create(path)?;
        memtable.wal = Some(wal);
        Ok(memtable)
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id,
            wal: Some(Wal::recover(path.as_ref(), &map)?),
            map,
            approximate_size: AtomicUsize::new(0),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_bound_keyslice(lower, TS_DEFAULT),
            map_bound_keyslice(upper, TS_DEFAULT),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        // SAFETY: Bytes只在函数内用，且Bytes析构后不会释放静态的切片，所以是安全的
        let key = unsafe { key.to_tmp_key_bytes() };
        self.map.get(&key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        for (k, v) in data {
            self.approximate_size
                .fetch_add(k.raw_len() + v.len(), Ordering::Relaxed);
            self.map.insert(k.to_key_bytes(), Bytes::copy_from_slice(v));

            if let Some(wal) = &self.wal {
                wal.put(*k, v)?;
            }
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter: MemTableIterator = MemTableIteratorBuilder {
            map: Arc::clone(&self.map),
            iter_builder: |map| {
                map.range((
                    lower.map(|key| key.to_key_bytes()),
                    upper.map(|key| key.to_key_bytes()),
                ))
            },
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<MemTableSnapshotIterator> {
        MemTableSnapshotIterator::new(self, lower, upper, read_ts)
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key().as_key_slice(), entry.value());
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.with_item(|item| item.1.as_ref())
    }

    fn key(&self) -> KeySlice {
        self.with_item(|item| item.0.as_key_slice())
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            *fields.item = match fields.iter.next() {
                Some(entry) => (entry.key().clone(), entry.value().clone()),
                None => (KeyBytes::new(), Bytes::new()),
            };
            Ok(())
        })
    }
}

pub struct MemTableSnapshotIterator {
    inner: MemTableIterator,
    upper: Bound<Bytes>,
    read_ts: u64,
    cur_key: Vec<u8>,
    valid: bool,
}

impl MemTableSnapshotIterator {
    pub fn new(
        table: &MemTable,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut inner: MemTableIterator = MemTableIteratorBuilder {
            map: Arc::clone(&table.map),
            iter_builder: |map| {
                map.range((
                    lower.map(|key| KeySlice::from_slice(key, TS_RANGE_BEGIN).to_key_bytes()),
                    upper.map(|key| KeySlice::from_slice(key, TS_RANGE_END).to_key_bytes()),
                ))
            },
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        inner.next()?;

        if let Bound::Excluded(key) = lower {
            while inner.is_valid() && inner.key().key_ref() == key {
                inner.next()?;
            }
        }

        let mut iter = Self {
            inner,
            upper: upper.map(Bytes::copy_from_slice),
            read_ts,
            cur_key: Vec::new(),
            valid: true,
        };
        iter.validate_upperbound();
        iter.seek_to_next_valid_key()?;

        Ok(iter)
    }

    fn validate_upperbound(&mut self) {
        if !self.inner.is_valid() || !self.valid {
            self.valid = false;
            return;
        }

        match &self.upper {
            Bound::Included(key) => {
                if self.inner.key().key_ref() > key {
                    self.valid = false;
                }
            }
            Bound::Excluded(key) => {
                if self.inner.key().key_ref() >= key {
                    self.valid = false;
                }
            }
            Bound::Unbounded => (),
        }
    }

    fn seek_to_next_valid_key(&mut self) -> Result<()> {
        while self.is_valid()
            && (self.inner.key().key_ref() == self.cur_key || self.inner.key().ts() > self.read_ts)
        {
            self.inner.next()?;
            self.validate_upperbound();
        }
        if self.is_valid() {
            self.cur_key.clear();
            self.cur_key.extend(self.inner.key().key_ref());
        }
        Ok(())
    }
}

impl StorageIterator for MemTableSnapshotIterator {
    type KeyType<'a> = <MemTableIterator as StorageIterator>::KeyType<'a>;

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.seek_to_next_valid_key()?;
        self.validate_upperbound();
        Ok(())
    }
}
