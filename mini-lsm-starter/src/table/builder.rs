#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{io::IoSlice, path::Path};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::table::FileObject;
use crate::{block::BlockBuilder, key::{KeyBytes, KeySlice}, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Bytes,
    last_key: Bytes,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Bytes::new(),
            last_key: Bytes::new(),
            data: Vec::with_capacity(256 * 1024 * 1024),
            meta: Vec::new(),
            block_size
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.encode_current_block_builder();
        }
        
        if self.first_key.is_empty() {
            self.first_key = Bytes::copy_from_slice(key.raw_ref());
        }
        self.last_key = Bytes::copy_from_slice(key.raw_ref());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.encode_current_block_builder();
        let meta_sec_off = self.data.len();
        let encoded_meta_sec_off = (meta_sec_off as u32).to_le_bytes();

        let mut encoded_meta_sec = Vec::<u8>::new();
        BlockMeta::encode_block_meta(&self.meta, &mut encoded_meta_sec);

        let file_object = FileObject::create(path.as_ref(), 
            &[IoSlice::new(&self.data), IoSlice::new(&encoded_meta_sec), 
            IoSlice::new(&encoded_meta_sec_off)])?;

        let first_key = self.meta.first().map_or(KeyBytes::default(), |meta| meta.first_key.clone());
        let last_key = self.meta.first().map_or(KeyBytes::default(), |meta| meta.last_key.clone());
        
        Ok(SsTable::create(file_object, self.meta, meta_sec_off, id, block_cache, first_key, last_key, None, 0))
    }

    fn encode_current_block_builder(&mut self) {
        if !self.builder.is_empty() {
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let meta = BlockMeta::new(self.data.len(), KeyBytes::from_bytes(self.first_key.clone()), KeyBytes::from_bytes(self.last_key.clone()));

            self.data.extend(builder.build().encode());
            self.meta.push(meta);

            self.first_key = Bytes::new();
        }
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
