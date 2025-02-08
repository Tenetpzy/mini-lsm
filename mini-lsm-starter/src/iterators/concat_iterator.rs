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

use anyhow::Result;
use bytes::Bytes;
use std::{ops::Bound, sync::Arc};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        match sstables.first() {
            Some(sstable) => {
                let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sstable))?;
                Ok(Self::create(iter, 1, sstables))
            }
            None => Ok(Self::create_invalid(sstables)),
        }
    }

    pub fn create_and_seek_to_first_with_ts(
        sstables: Vec<Arc<SsTable>>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = Self::create_and_seek_to_first(sstables)?;
        if !iter.try_seek_to_cur_key_with_ts(read_ts)? {
            iter.seek_to_next_key_with_ts(read_ts)?;
        }
        Ok(iter)
    }

    /// seek to the first element which >= key
    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = match sstables.binary_search_by(|sst| sst.cmp_range_with_key(key)) {
            Ok(idx) | Err(idx) => idx,
        };

        if idx >= sstables.len() {
            Ok(Self::create_invalid(sstables))
        } else {
            // 这个iter一定是有效的，因为上面二分搜索时sst区间是闭区间
            let iter = SsTableIterator::create_and_seek_to_key(Arc::clone(&sstables[idx]), key)?;
            Ok(Self::create(iter, idx + 1, sstables))
        }
    }

    pub fn create_and_seek_to_key_with_ts(
        sstables: Vec<Arc<SsTable>>,
        key: KeySlice,
    ) -> Result<Self> {
        let mut iter = Self::create_and_seek_to_key(sstables, key)?;
        if !iter.try_seek_to_cur_key_with_ts(key.ts())? {
            iter.seek_to_next_key_with_ts(key.ts())?;
        }
        Ok(iter)
    }

    fn seek_to_next_key_with_ts(&mut self, read_ts: u64) -> Result<()> {
        if !self.is_valid() {
            Ok(())
        } else {
            let sst_iter = self.current.as_mut().unwrap();
            sst_iter.seek_to_next_key_with_ts(read_ts)?;
            if !sst_iter.is_valid() {
                self.seek_to_next_sst_with_ts(read_ts)
            } else {
                Ok(())
            }
        }
    }

    fn try_seek_to_cur_key_with_ts(&mut self, read_ts: u64) -> Result<bool> {
        match &mut self.current {
            Some(sst_iter) => {
                // compaction保证了一个key的多版本只会在一个SST中，这里只用做一次
                sst_iter.try_seek_to_cur_key_with_ts(read_ts)
            }
            None => Ok(false),
        }
    }

    fn create_invalid(sstables: Vec<Arc<SsTable>>) -> Self {
        SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        }
    }

    fn create(iter: SsTableIterator, next_sst_idx: usize, sstables: Vec<Arc<SsTable>>) -> Self {
        SstConcatIterator {
            current: Some(iter),
            next_sst_idx,
            sstables,
        }
    }

    fn set_invalid(&mut self) {
        self.current = None;
    }

    fn seek_to_idx(&mut self, idx: usize) -> Result<()> {
        if idx >= self.sstables.len() {
            self.set_invalid();
            Ok(())
        } else {
            let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(&self.sstables[idx]))?;
            self.current = Some(iter);
            self.next_sst_idx = idx + 1;
            Ok(())
        }
    }

    /// 定位到下一个，能够从中找到一个满足时间戳read_ts的键的SST，或者定位到末尾
    fn seek_to_next_sst_with_ts(&mut self, read_ts: u64) -> Result<()> {
        if self.next_sst_idx >= self.sstables.len() {
            self.set_invalid();
            Ok(())
        } else {
            let iter = SsTableIterator::create_and_seek_to_first_with_ts(
                Arc::clone(&self.sstables[self.next_sst_idx]),
                read_ts,
            )?;
            self.next_sst_idx += 1;
            if iter.is_valid() {
                self.current = Some(iter);
                Ok(())
            } else {
                self.seek_to_next_key_with_ts(read_ts)
            }
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current {
            iter.next()?;
            if !iter.is_valid() {
                self.seek_to_idx(self.next_sst_idx)?;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}

pub struct SstRangeConcatSnapshotIterator {
    iter_inner: SstConcatIterator,
    upper: Bound<Bytes>,
    read_ts: u64,
}

impl SstRangeConcatSnapshotIterator {
    pub fn create(
        sstables: Vec<Arc<SsTable>>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter_inner = match lower {
            Bound::Unbounded => {
                SstConcatIterator::create_and_seek_to_first_with_ts(sstables, read_ts)?
            }
            Bound::Included(key) | Bound::Excluded(key) => {
                SstConcatIterator::create_and_seek_to_key_with_ts(
                    sstables,
                    KeySlice::from_slice(key, read_ts),
                )?
            }
        };
        if let Bound::Excluded(key) = lower {
            if iter_inner.is_valid() && iter_inner.key().key_ref() == key {
                iter_inner.seek_to_next_key_with_ts(read_ts)?;
            }
        }

        let mut iter = Self {
            iter_inner,
            upper: upper.map(Bytes::copy_from_slice),
            read_ts,
        };
        iter.check_upper_bound();
        Ok(iter)
    }

    fn check_upper_bound(&mut self) {
        if self.iter_inner.is_valid() {
            match &self.upper {
                Bound::Unbounded => (),
                Bound::Included(key) => {
                    if self.iter_inner.key().key_ref() > key {
                        self.iter_inner.set_invalid();
                    }
                }
                Bound::Excluded(key) => {
                    if self.iter_inner.key().key_ref() >= key {
                        self.iter_inner.set_invalid();
                    }
                }
            }
        }
    }
}

impl StorageIterator for SstRangeConcatSnapshotIterator {
    type KeyType<'a> = <SstConcatIterator as StorageIterator>::KeyType<'a>;

    fn value(&self) -> &[u8] {
        self.iter_inner.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter_inner.key()
    }

    fn is_valid(&self) -> bool {
        self.iter_inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter_inner.seek_to_next_key_with_ts(self.read_ts)?;
        self.check_upper_bound();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter_inner.num_active_iterators()
    }
}
