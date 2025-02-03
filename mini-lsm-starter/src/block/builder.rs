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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, mut key: KeySlice, value: &[u8]) -> bool {
        if self.offsets.len() == u16::MAX as usize {
            return false;
        }
        let val_len = value.len();
        if val_len > u16::MAX.into() {
            return false;
        }

        let key_overlap_len: usize;
        let key_rest_len: usize;

        if self.first_key.is_empty() {
            self.first_key.append(key.raw_ref());
            key_overlap_len = 0;
            key_rest_len = key.len();
        } else {
            // 不是第一个key，使用前缀压缩，只记录和第一个key不同的后缀部分
            let key_len = key.len();
            key = self.remove_common_prefix_of_first_key(key);
            key_rest_len = key.len();

            // 现在可以做容量检查了
            if key_rest_len > u16::MAX.into() {
                return false;
            }
            // 4: key_overlap_len(u16) + key_rest_len(u16) + value_len(u16) + offset(u16)
            let new_entry_extra_len = key_rest_len + val_len + 4 * size_of::<u16>();
            if self.current_size() + new_entry_extra_len > self.block_size {
                return false;
            }

            key_overlap_len = key_len - key_rest_len;
        }

        self.offsets.push(self.data.len() as u16);

        // key_len and val_len are encoded as little-endian
        self.data.put_u16_le(key_overlap_len as u16);
        self.data.put_u16_le(key_rest_len as u16);
        self.data.put(key.raw_ref());
        self.data.put_u16_le(val_len as u16);
        self.data.put(value);

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

    pub fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * size_of::<u16>() + size_of::<u16>()
    }

    /// 将key和first_key的最长公共前缀从key中移除，返回后缀
    fn remove_common_prefix_of_first_key<'a>(&self, key: KeySlice<'a>) -> KeySlice<'a> {
        let prefix_len = key
            .raw_ref()
            .iter()
            .zip(self.first_key.raw_ref())
            .take_while(|(a, b)| a == b)
            .count();
        KeySlice::from_slice(&key.raw_ref()[prefix_len..])
    }
}
