#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    // /// The first key in the block
    // first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.update_key_val_to_idx();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.update_key_val_to_idx();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.idx = self.block.binary_search_key_index(key);
        self.update_key_val_to_idx();
    }

    fn update_key_val_to_idx(&mut self) {
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
        } else {
            self.key = self.block.get_key_on_idx(self.idx).to_key_vec();
            self.value_range = self.block.get_value_range_on_idx(self.idx);
        }
    }
}

impl Block {
    // returns the index of the first key that >= param key
    fn binary_search_key_index(&self, key: KeySlice) -> usize {
        let mut start: usize = 0;
        let mut end = self.offsets.len();

        while start < end {
            let mid = start + (end - start) / 2;
            let mid_key = self.get_key_on_idx(mid);

            if key <= mid_key {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        start
    }

    fn get_key_on_idx(&self, idx: usize) -> KeySlice {
        let off = self.offsets[idx] as usize;
        let key_len = u16::from_le_bytes([self.data[off], self.data[off + 1]]) as usize;
        let key_start = off + 2;
        KeySlice::from_slice(&self.data[key_start..key_start + key_len])
    }

    fn get_value_range_on_idx(&self, idx: usize) -> (usize, usize) {
        let off = self.offsets[idx] as usize;
        let key_len = u16::from_le_bytes([self.data[off], self.data[off + 1]]) as usize;
        let off = off + 2 + key_len;
        let val_len = u16::from_le_bytes([self.data[off], self.data[off + 1]]) as usize;
        (off + 2, off + 2 + val_len)
    }
}
