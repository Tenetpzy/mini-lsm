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

use anyhow::{anyhow, Result};

use crate::{
    iterators::{
        concat_iterator::SstRangeConcatSnapshotIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableSnapshotIterator,
    table::SSTRangeSnapshotIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    MergeIterator<MemTableSnapshotIterator>, // memtable and immutable memtables
    TwoMergeIterator<
        MergeIterator<SSTRangeSnapshotIterator>,       // L0 SSTs
        MergeIterator<SstRangeConcatSnapshotIterator>, // sorted run SSTs
    >,
>;

/// LSM get/scan接口使用的迭代器
/// 1. 对任何一个键，只返回指定的read_ts下的最新版本
/// 2. 如果最新版本是删除，则不返回这个键
pub struct LsmIterator {
    inner: LsmIteratorInner,
    cur_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        let mut lsm_iter = Self {
            inner: iter,
            cur_key: Vec::new(),
        };
        lsm_iter.seek_to_next_non_empty_key()?;
        Ok(lsm_iter)
    }

    fn seek_to_next_non_empty_key(&mut self) -> Result<()> {
        self.pass_older_version_of_currnet_key()?;

        // 去掉被删除的键
        while self.inner.is_valid() {
            self.cur_key.clear();
            self.cur_key.extend(self.inner.key().key_ref());

            if self.inner.value().is_empty() {
                self.pass_older_version_of_currnet_key()?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn pass_older_version_of_currnet_key(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.key().key_ref() == self.cur_key {
            self.inner.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.seek_to_next_non_empty_key()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored {
            panic!("use invalid iter");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored {
            panic!("use invalid iter");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            Err(anyhow!("iter is already errored"))
        } else if self.iter.is_valid() {
            match self.iter.next() {
                Ok(_) => Ok(()),
                Err(e) => {
                    self.has_errored = true;
                    Err(e)
                }
            }
        } else {
            Ok(())
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
