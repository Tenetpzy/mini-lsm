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

use std::{
    cmp::{Ordering, Reverse},
    sync::Arc,
};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
#[derive(Clone)]
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
        // let first_key = block.get_key_on_idx(0);
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            // first_key,
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
        self.seek_to_index(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_index(self.block.binary_search_key_index(key));
    }

    // /// 定位到下一个值不同的，小于读时间戳read_ts的具有最大时间戳的key。用于MVCC读的优化。
    // pub fn seek_to_next_key_with_ts(&mut self, read_ts: u64) {
    //     loop {
    //         self.seek_to_next_key();
    //         if self.try_seek_to_cur_key_with_ts(read_ts) {
    //             break;
    //         } else if !self.is_valid() {
    //             break;
    //         }
    //     }
    // }

    // /// 定位到下一个值不同的key的最新版本，而不是相同key的旧版本
    // pub fn seek_to_next_key(&mut self) {
    //     if self.is_valid() {
    //         // 查找大于(cur_key_ref, TS_RANGE_END)的键，即是下一个值不同的key
    //         let key = KeySlice::from_slice(self.key.key_ref(), TS_RANGE_END);
    //         let mut idx = self.block.binary_search_key_index(key);
    //         if idx < self.block.offsets.len() {
    //             if self.block.get_key_silce_on_idx(idx).iter().eq(key.key_ref()) {
    //                 idx += 1;
    //                 if idx < self.block.offsets.len() {
    //                     assert!(!self.block.get_key_silce_on_idx(idx).iter().eq(key.key_ref()));
    //                 }
    //             }
    //         }
    //         self.seek_to_index(idx);
    //     }
    // }

    // /// 尝试定位到当前key满足读时间戳read_ts的最大版本。如果成功返回true，如果不存在这样的版本，迭代器不移动并返回false
    // pub fn try_seek_to_cur_key_with_ts(&mut self, read_ts: u64) -> bool {
    //     if self.is_valid() {
    //         let key = KeySlice::from_slice(self.key.key_ref(), read_ts);
    //         let idx = self.block.binary_search_key_index(key);
    //         if idx < self.block.offsets.len() {
    //             if self.block.get_key_silce_on_idx(idx).iter().eq(key.key_ref()) {
    //                 // 找到符合的版本了
    //                 self.seek_to_index(idx);
    //                 true
    //             } else {
    //                 false
    //             }
    //         } else {
    //             false
    //         }
    //     } else {
    //         false
    //     }
    // }

    fn seek_to_index(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
        } else {
            self.idx = idx;
            self.key = self.block.get_key_on_idx(self.idx);
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
            let mid_key = self.get_key_silce_on_idx(mid);

            if mid_key >= key {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        start
    }

    fn get_key_on_idx(&self, idx: usize) -> KeyVec {
        self.get_key_silce_on_idx(idx).to_key_vec()
    }

    // kv encoding:
    // key_overlap_len(u16) | key_rest_len(u16) | key(key_rest_len) | timestamp(u64) | val_len(u16) | value(val_len)
    fn get_value_range_on_idx(&self, idx: usize) -> (usize, usize) {
        let off = self.offsets[idx] as usize;
        let key_rest_len = (&self.data[off + 2..off + 4]).get_u16_le() as usize;
        let off = off + 12 + key_rest_len; // 12: u16 + u16 + u64
        let val_len = (&self.data[off..off + 2]).get_u16_le() as usize;
        (off + 2, off + 2 + val_len)
    }

    fn get_key_silce_on_idx(&self, idx: usize) -> CompressedKeySlice {
        let off = self.offsets[idx] as usize;
        let key_overlap_len = (&self.data[off..off + 2]).get_u16_le() as usize;
        let key_rest_len = (&self.data[off + 2..off + 4]).get_u16_le() as usize;
        let ts_off = off + 4 + key_rest_len;
        let ts = (&self.data[ts_off..ts_off + 8]).get_u64_le();
        let first_key_off = self.offsets[0] as usize + 4;
        let key_off = off + 4;
        CompressedKeySlice {
            prefix: &self.data[first_key_off..first_key_off + key_overlap_len],
            suffix: &self.data[key_off..key_off + key_rest_len],
            ts,
        }
    }
}

/// 压缩后的key的引用，内部保存指向first_key前缀的引用和key本身后缀的引用
struct CompressedKeySlice<'a> {
    prefix: &'a [u8],
    suffix: &'a [u8],
    ts: u64,
}

impl CompressedKeySlice<'_> {
    fn iter(&self) -> impl Iterator<Item = &u8> {
        self.prefix.iter().chain(self.suffix.iter())
    }

    fn to_key_vec(&self) -> KeyVec {
        let mut keyvec = KeyVec::new();
        keyvec.append(self.prefix);
        keyvec.append(self.suffix);
        keyvec.set_ts(self.ts);
        keyvec
    }
}

impl PartialEq<KeySlice<'_>> for CompressedKeySlice<'_> {
    fn eq(&self, other: &KeySlice<'_>) -> bool {
        // self.iter().eq(other.raw_ref().iter())
        self.iter().eq(other.key_ref().iter()) && self.ts == other.ts()
    }
}

impl PartialOrd<KeySlice<'_>> for CompressedKeySlice<'_> {
    fn partial_cmp(&self, other: &KeySlice<'_>) -> Option<Ordering> {
        // 这里需要手动实现字典序，因为self.iter()返回的迭代器类型是自动推断的，没有为它实现比较方法，否则可以用元组
        self.iter()
            .partial_cmp(other.key_ref().iter())
            .and_then(|ord| {
                if ord == Ordering::Equal {
                    Reverse(self.ts).partial_cmp(&Reverse(other.ts()))
                } else {
                    Some(ord)
                }
            })
    }
}
