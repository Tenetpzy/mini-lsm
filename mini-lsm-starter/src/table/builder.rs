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

use std::io::IoSlice;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{BlockMeta, SsTMetaInfo, SsTable};
use crate::table::FileObject;
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyBytes,
    last_key: KeyBytes,
    max_ts: u64,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    key_hashs: Vec<u32>,
    bloom_false_positive_rate: f64,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyBytes::new(),
            last_key: KeyBytes::new(),
            max_ts: 0,
            data: Vec::with_capacity(256 * 1024 * 1024),
            meta: Vec::new(),
            key_hashs: Vec::new(),
            bloom_false_positive_rate: 0.01,
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.encode_current_block_builder();
            let res = self.builder.add(key, value);
            assert!(res);
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_key_bytes();
        }
        self.last_key = key.to_key_bytes();
        self.max_ts = self.max_ts.max(key.ts());
        self.key_hashs.push(SsTable::key_hash(key.key_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.current_size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.encode_current_block_builder();

        let block_section_end_offset = self.data.len() as u64;
        let block_meta_offset = self.data.len() as u64;

        let mut encoded_block_meta = Vec::<u8>::new();
        BlockMeta::encode_block_meta(&self.meta, &mut encoded_block_meta);
        let encoded_block_meta_off = (block_meta_offset as u32).to_le_bytes();
        let block_meta_len = encoded_block_meta.len() as u64;

        let bloom_filter_offset = block_meta_offset + block_meta_len + 4;
        let mut encoded_bloom_filter = Vec::<u8>::new();
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashs,
            Bloom::bloom_bits_per_key(self.key_hashs.len(), self.bloom_false_positive_rate),
        );
        bloom.encode(&mut encoded_bloom_filter);
        let bloom_filter_len = encoded_bloom_filter.len() as u64;
        let encoded_bloom_filter_off = (bloom_filter_offset as u32).to_le_bytes();

        let encoded_max_ts = self.max_ts.to_le_bytes();

        let file_object = FileObject::create(
            path.as_ref(),
            &[
                IoSlice::new(&self.data),
                IoSlice::new(&encoded_block_meta),
                IoSlice::new(&encoded_block_meta_off),
                IoSlice::new(&encoded_bloom_filter),
                IoSlice::new(&encoded_bloom_filter_off),
                IoSlice::new(&encoded_max_ts),
            ],
        )?;

        let meta_info: SsTMetaInfo = SsTMetaInfo {
            block_meta_offset,
            block_meta_len,
            bloom_filter_offset,
            bloom_filter_len,
            block_section_end_offset,
        };

        Ok(SsTable::new(
            file_object,
            self.meta,
            meta_info,
            id,
            block_cache,
            Some(bloom),
            self.max_ts,
        ))
    }

    fn encode_current_block_builder(&mut self) {
        if !self.builder.is_empty() {
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let meta = BlockMeta::new(
                self.data.len(),
                self.first_key.clone(),
                self.last_key.clone(),
            );

            self.data.put(builder.build().encode());
            self.meta.push(meta);

            self.first_key = KeyBytes::new();
        }
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
