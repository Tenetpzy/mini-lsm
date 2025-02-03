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

use anyhow::{ensure, Result};
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
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

        block.put(&self.data[..]);
        for offset in &self.offsets {
            block.put_u16_le(*offset);
        }
        block.put_u16_le(self.offsets.len() as u16);

        // 生成块的校验和
        let checksum = crc32fast::hash(&block);
        block.put_u32_le(checksum);

        block.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Result<Self> {
        let block_len = data.len() - 4;
        let checksum = (&data[data.len() - 4..]).get_u32_le();
        ensure!(
            crc32fast::hash(&data[..data.len() - 4]) == checksum,
            "Block checksum mismatch, block corrupted?"
        );

        // decode element number
        let elem_num = (&data[block_len - 2..block_len]).get_u16_le() as usize;

        // decode offsets
        let offset_end_idx = block_len - 2;
        let offset_start_idx = offset_end_idx - 2 * elem_num;
        let offset_part = &data[offset_start_idx..offset_end_idx];

        let offsets: Vec<u16> = offset_part
            .chunks_exact(2)
            .map(|mut offset_byte| offset_byte.get_u16_le())
            .collect();

        // decode KVs
        let kv_data = Vec::from(&data[..offset_start_idx]);

        Ok(Block {
            data: kv_data,
            offsets,
        })
    }
}
