#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::KeySlice;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    // /// The first key in the block
    // first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            // first_key: KeyVec::new()
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.offsets.len() == u16::MAX.into() {
            return false;
        }

        let key_len = key.len();
        let val_len = value.len();

        if key_len > u16::MAX.into() || val_len > u16::MAX.into() {
            return false;
        }

        let new_entry_extra_len = key_len + val_len + 3 * size_of::<u16>();
        if (self.current_size() + new_entry_extra_len > self.block_size) && !self.offsets.is_empty()
        {
            return false;
        }

        let key_len = key_len as u16;
        let val_len = val_len as u16;

        self.offsets.push(self.data.len() as u16);

        // key_len and val_len are encoded as little-endian
        self.data.extend(key_len.to_le_bytes());
        self.data.extend(key.raw_ref());
        self.data.extend(val_len.to_le_bytes());
        self.data.extend(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * size_of::<u16>() + size_of::<u16>()
    }
}
