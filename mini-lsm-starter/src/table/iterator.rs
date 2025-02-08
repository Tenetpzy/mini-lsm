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
use std::ops::Bound;
use std::sync::Arc;

use super::SsTable;
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{KeySlice, TS_RANGE_END},
};

/// An iterator over the contents of an SSTable.
#[derive(Clone)]
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: Option<BlockIterator>,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iter = SsTableIterator {
            table,
            blk_iter: None,
            blk_idx: 0,
        };
        iter.seek_to_first()?;
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        if self.table.num_of_blocks() > 0 {
            let block = self.table.read_block_cached(0)?;
            self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
        }
        Ok(())
    }

    /// 创建并定位到第一个满足读时间戳read_ts的键
    pub fn create_and_seek_to_first_with_ts(table: Arc<SsTable>, read_ts: u64) -> Result<Self> {
        let mut iter = Self::create_and_seek_to_first(table)?;
        if !iter.try_seek_to_cur_key_with_ts(read_ts)? {
            iter.seek_to_next_key_with_ts(read_ts)?;
        }
        Ok(iter)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = SsTableIterator {
            table,
            blk_iter: None,
            blk_idx: 0,
        };
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// 创建并定位到第一个键值 >= key且读时间戳满足key.ts()的键   
    /// 注意和create_and_seek_to_key的区别，create_and_seek_to_key不考虑是否满足读时间戳
    pub fn create_and_seek_to_key_with_ts(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self::create_and_seek_to_key(table, key)?;
        if !iter.try_seek_to_cur_key_with_ts(key.ts())? {
            iter.seek_to_next_key_with_ts(key.ts())?;
        }
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let blk_idx = self.table.find_block_idx(key);
        if blk_idx < self.table.num_of_blocks() {
            if blk_idx != self.blk_idx || self.blk_iter.is_none() {
                let block = self.table.read_block_cached(blk_idx)?;
                self.blk_iter = Some(BlockIterator::create_and_seek_to_key(block, key));
            } else {
                self.blk_iter.as_mut().unwrap().seek_to_key(key);
            }
        }
        self.blk_idx = blk_idx;
        Ok(())
    }

    /// 定位到下一个值不同的，小于读时间戳read_ts的具有最大时间戳的key。用于MVCC快照读的优化。
    pub fn seek_to_next_key_with_ts(&mut self, read_ts: u64) -> Result<()> {
        let mut cur_key: Vec<u8> = Vec::new();

        while self.is_valid() {
            cur_key.clear();
            cur_key.extend(self.key().key_ref());

            // 使用(cur_key, TS_RANGE_END)快速跳过当前key的其它版本，此时最差情况指向最旧版本
            self.seek_to_key(KeySlice::from_slice(&cur_key, TS_RANGE_END))?;
            if self.is_valid() {
                // 判断是否指向最旧版本，如果是，再next一下就指向了下一个key
                if self.key().key_ref() == cur_key {
                    self.next()?;

                    // 正确性测试
                    if self.is_valid() {
                        assert!(
                            self.key().key_ref() != cur_key,
                            "iter key: {:?}, ts: {:?}, cur_key: {:?}",
                            self.key().key_ref(),
                            self.key().ts(),
                            cur_key
                        );
                    }
                }

                if self.is_valid() {
                    // 如果当前键有满足要求的版本，可以返回，否则继续循环寻找下一个键
                    if self.try_seek_to_cur_key_with_ts(read_ts)? {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    /// 定位到当前key，满足读时间戳ts的版本。如果能找到，返回true，否则不更改迭代器返回false
    pub fn try_seek_to_cur_key_with_ts(&mut self, read_ts: u64) -> Result<bool> {
        if !self.is_valid() {
            return Ok(false);
        }

        let cur_key = self.key();
        let mut tmp = self.clone();
        tmp.seek_to_key(KeySlice::from_slice(cur_key.key_ref(), read_ts))?;
        if tmp.is_valid() {
            let tmp_key = tmp.key();
            if tmp_key.key_ref() == cur_key.key_ref() {
                assert!(tmp_key.ts() <= read_ts);
                *self = tmp;
                return Ok(true);
            } else {
                return Ok(false);
            }
        }

        Ok(false)
    }

    fn seek_to_next_blk(&mut self) -> Result<()> {
        self.blk_idx += 1;

        if self.blk_idx >= self.table.num_of_blocks() {
            // 遍历SST结束了
            self.blk_iter = None;
        } else {
            // 还有下一个块
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
        }

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.as_ref().unwrap().key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.as_ref().unwrap().value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_idx < self.table.num_of_blocks()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.blk_iter {
            iter.next();
            if !iter.is_valid() {
                self.seek_to_next_blk()?;
            }
        }

        Ok(())
    }
}

/// 对于范围内每个键，只返回小于等于read ts的最大时间戳版本
pub struct SSTRangeSnapshotIterator {
    iter: SsTableIterator,
    upper: Bound<Bytes>,
    valid: bool,
    read_ts: u64,
}

impl SSTRangeSnapshotIterator {
    pub fn create(
        table: Arc<SsTable>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = match lower {
            Bound::Included(key) | Bound::Excluded(key) => {
                SsTableIterator::create_and_seek_to_key_with_ts(
                    table,
                    KeySlice::from_slice(key, read_ts),
                )?
            }
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first_with_ts(table, read_ts)?,
        };
        if let Bound::Excluded(key) = lower {
            if iter.is_valid() && iter.key().key_ref() == key {
                iter.seek_to_next_key_with_ts(read_ts)?;
            }
        }

        let mut range_iter = Self {
            iter,
            upper: upper.map(Bytes::copy_from_slice),
            valid: true,
            read_ts,
        };
        range_iter.validate_upper_bound();
        Ok(range_iter)
    }

    // 检查当前是否超过了upper，如果是，置valid为false
    fn validate_upper_bound(&mut self) {
        if !self.iter.is_valid() || !self.valid {
            self.valid = false;
            return;
        }

        match &self.upper {
            Bound::Included(key) => {
                if self.iter.key().key_ref() > key {
                    self.valid = false;
                }
            }
            Bound::Excluded(key) => {
                if self.iter.key().key_ref() >= key {
                    self.valid = false;
                }
            }
            Bound::Unbounded => {}
        }
    }
}

impl StorageIterator for SSTRangeSnapshotIterator {
    type KeyType<'a> = <SsTableIterator as StorageIterator>::KeyType<'a>;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn next(&mut self) -> Result<()> {
        self.iter.seek_to_next_key_with_ts(self.read_ts)?;
        self.validate_upper_bound();
        Ok(())
    }
}
