use std::{ops::Bound, sync::Arc};

use anyhow::Result;

use super::SsTable;
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{KeyBytes, KeySlice},
    mem_table::map_bound_keybytes,
};

/// An iterator over the contents of an SSTable.
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

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = self.table.find_block_idx(key);
        if self.blk_idx < self.table.num_of_blocks() {
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = Some(BlockIterator::create_and_seek_to_key(block, key));
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter
            .as_ref()
            .map_or(KeySlice::from_slice(b""), |iter| iter.key())
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter
            .as_ref()
            .map_or(b"".as_ref(), |iter| iter.value())
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
                self.blk_idx += 1;

                if self.blk_idx >= self.table.num_of_blocks() {
                    // 遍历SST结束了
                    self.blk_iter = None;
                } else {
                    // 还有下一个块
                    let block = self.table.read_block_cached(self.blk_idx)?;
                    self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
                }
            }
        }

        Ok(())
    }
}

pub struct SSTRangeIterator {
    iter: SsTableIterator,
    upper: Bound<KeyBytes>,
    valid: bool,
}

impl SSTRangeIterator {
    pub fn create(table: Arc<SsTable>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<Self> {
        let mut iter = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => {
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
            }
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
        };
        if let Bound::Excluded(key) = &lower {
            if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                iter.next()?;
            }
        }

        let mut range_iter = Self {
            iter,
            upper: map_bound_keybytes(upper),
            valid: true,
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
                if self.iter.key() > key.as_key_slice() {
                    self.valid = false;
                }
            }
            Bound::Excluded(key) => {
                if self.iter.key() >= key.as_key_slice() {
                    self.valid = false;
                }
            }
            Bound::Unbounded => {}
        }
    }
}

impl StorageIterator for SSTRangeIterator {
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
        self.iter.next()?;
        self.validate_upper_bound();
        Ok(())
    }
}
