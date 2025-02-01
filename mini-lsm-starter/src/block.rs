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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut block = BytesMut::new();

        block.extend(&self.data);
        for offset in &self.offsets {
            block.extend(offset.to_le_bytes());
        }
        block.extend((self.offsets.len() as u16).to_le_bytes());

        block.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let block_len = data.len();

        // decode element number
        let elem_num = u16::from_le_bytes([data[block_len - 2], data[block_len - 1]]) as usize;

        // decode offsets
        let offset_end_idx = block_len - 2;
        let offset_start_idx = offset_end_idx - 2 * elem_num;
        let offset_part = &data[offset_start_idx..offset_end_idx];

        let offsets: Vec<u16> = offset_part
            .chunks_exact(2)
            .map(|offset_byte| u16::from_le_bytes([offset_byte[0], offset_byte[1]]))
            .collect();

        // decode KVs
        let kv_data = Vec::from(&data[..offset_start_idx]);

        Block {
            data: kv_data,
            offsets,
        }
    }
}
