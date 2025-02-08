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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering;
use std::fs::File;
use std::io::{IoSlice, Write};
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SSTRangeSnapshotIterator;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    pub fn new(offset: usize, first_key: KeyBytes, last_key: KeyBytes) -> BlockMeta {
        Self {
            offset,
            first_key,
            last_key,
        }
    }

    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.  
    /// encode format:  
    /// BlockMeta_number (le u64) | BlockMeta | ... | BlockMeta | checksum(le u64)
    /// each BlockMeta:
    /// offset(le u64) | first_key_len(le u64) | first_key | first_key_ts(le u64) | last_key_len(le u64) | last_key | last_key_ts(le u64)
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.put_u64_le(block_meta.len() as u64);
        for meta in block_meta {
            buf.put_u64_le(meta.offset as u64);

            buf.put_u64_le(meta.first_key.key_len() as u64);
            buf.put(meta.first_key.key_ref());
            buf.put_u64_le(meta.first_key.ts());

            buf.put_u64_le(meta.last_key.key_len() as u64);
            buf.put(meta.last_key.key_ref());
            buf.put_u64_le(meta.last_key.ts());
        }

        // 增加block meta区域校验和
        let checksum = crc32fast::hash(buf);
        buf.put_u32_le(checksum);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let checksum = (&buf[buf.len() - 4..]).get_u32_le();
        ensure!(
            crc32fast::hash(&buf[..buf.len() - 4]) == checksum,
            "Block Meta checksum mismatch, block meta corrupted?"
        );

        let cnt = buf.get_u64_le();
        let mut res: Vec<BlockMeta> = Vec::new();

        for _ in 0..cnt {
            let offset = buf.get_u64_le();

            let first_key_len = buf.get_u64_le();
            let first_key = buf.copy_to_bytes(first_key_len as usize);
            let first_key_ts = buf.get_u64_le();
            let first_key = KeyBytes::from_bytes_with_ts(first_key, first_key_ts);

            let last_key_len = buf.get_u64_le();
            let last_key = buf.copy_to_bytes(last_key_len as usize);
            let last_key_ts = buf.get_u64_le();
            let last_key = KeyBytes::from_bytes_with_ts(last_key, last_key_ts);

            res.push(BlockMeta::new(offset as usize, first_key, last_key));
        }

        Ok(res)
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, datas: &[IoSlice]) -> Result<Self> {
        let mut file = File::create(path)?;

        let data_len: usize = datas.iter().map(|slice| slice.len()).sum();
        let write_len = file.write_vectored(datas)?;
        if write_len != data_len {
            return Err(anyhow!("partial write SST file"));
        }

        file.sync_all()?;
        let len = file.metadata()?.len();
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            len,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

#[derive(Default)]
pub(crate) struct SsTMetaInfo {
    block_meta_offset: u64,
    block_meta_len: u64,
    bloom_filter_offset: u64,
    bloom_filter_len: u64,
    block_section_end_offset: u64, // 数据区域的尾后偏移
}

/// An SSTable.
/// SST structure: reference to tutorial 1.7
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    meta_info: SsTMetaInfo,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let meta_info = Self::decode_meta_info(&file)?;

        let block_meta_buf = file.read(meta_info.block_meta_offset, meta_info.block_meta_len)?;
        let block_meta = BlockMeta::decode_block_meta(&block_meta_buf[..])?;

        let bloom_buf = file.read(meta_info.bloom_filter_offset, meta_info.bloom_filter_len)?;
        let bloom = Bloom::decode(&bloom_buf)?;

        Ok(Self::new(
            file,
            block_meta,
            meta_info,
            id,
            block_cache,
            Some(bloom),
            0,
        ))
    }

    /// Also used in SsTableBuilder:build, avoid repeatly decoding meta from file.
    pub(crate) fn new(
        file: FileObject,
        block_meta: Vec<BlockMeta>,
        meta_info: SsTMetaInfo,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        bloom: Option<Bloom>,
        max_ts: u64,
    ) -> Self {
        let first_key = block_meta
            .first()
            .map_or(KeyBytes::default(), |meta| meta.first_key.clone());
        let last_key = block_meta
            .last()
            .map_or(KeyBytes::default(), |meta| meta.last_key.clone());
        Self {
            file,
            block_meta,
            meta_info,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts,
        }
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            meta_info: SsTMetaInfo::default(),
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            return Err(anyhow!("block index {block_idx} out of range"));
        }

        // block meta中没有记录block的大小或尾偏移，这里计算出来
        let block_end = if block_idx == self.block_meta.len() - 1 {
            self.meta_info.block_section_end_offset as usize
        } else {
            self.block_meta[block_idx + 1].offset
        };
        let block_start = self.block_meta[block_idx].offset;
        assert!(block_end >= block_start);
        let block_len = block_end - block_start;

        let block = self.file.read(block_start as u64, block_len as u64)?;
        let block = Block::decode(&block)?;
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        self.block_cache
            .as_ref()
            .map_or(self.read_block(block_idx), |cache| {
                cache
                    .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
                    .map_err(|err| anyhow!(err))
            })
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    ///
    /// 返回的block idx，要么是end，要么是SST中第一个满足key值 >= 参数key的键所在块
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        match self.block_meta.binary_search_by(|meta| {
            if key < meta.first_key.as_key_slice() {
                Ordering::Greater
            } else if key > meta.last_key.as_key_slice() {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            Ok(idx) | Err(idx) => idx,
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    fn decode_meta_info(file: &FileObject) -> Result<SsTMetaInfo> {
        let encode_bloom_offset = file.read(file.size() - 4, 4)?;
        let bloom_filter_offset = (&encode_bloom_offset[..]).get_u32_le() as u64;
        let bloom_filter_len = file.size() - 4 - bloom_filter_offset;

        let encode_meta_offset = file.read(bloom_filter_offset - 4, 4)?;
        let block_meta_offset = (&encode_meta_offset[..]).get_u32_le() as u64;
        let block_meta_len = bloom_filter_offset - 4 - block_meta_offset;

        Ok(SsTMetaInfo {
            block_meta_offset,
            block_meta_len,
            bloom_filter_offset,
            bloom_filter_len,
            block_section_end_offset: block_meta_offset,
        })
    }

    /// 检查本SST的`[first_key, last_key]`和参数key的大小关系  
    /// `first_key > key`为greater, `last_key < key`为less, `first_key <= key <= last_key`为equal
    pub(crate) fn cmp_range_with_key(&self, key: KeySlice) -> Ordering {
        if self.first_key.as_key_slice() > key {
            Ordering::Greater
        } else if self.last_key.as_key_slice() < key {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }

    /// 检查[start, end]是否与本sst的`[first_key, last_key]`有交叠，有返回true
    fn range_overlap(&self, start: Bound<KeySlice>, end: Bound<KeySlice>) -> bool {
        let on_right = match start {
            Bound::Included(start) => start > self.last_key.as_key_slice(),
            Bound::Excluded(start) => start >= self.last_key.as_key_slice(),
            Bound::Unbounded => false,
        };
        let on_left = match end {
            Bound::Included(end) => end < self.first_key.as_key_slice(),
            Bound::Excluded(end) => end <= self.first_key.as_key_slice(),
            Bound::Unbounded => false,
        };

        !(on_left || on_right)
    }

    /// 检查本SST是否有可能包含[lower, upper]中一部分键
    pub(crate) fn may_contain(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> bool {
        if !self.range_overlap(lower, upper) {
            false
        } else {
            // 检查这个范围是否只是单个键值的不同版本（由get调用），此时可以用布隆过滤器优化
            let single_key = match lower {
                Bound::Excluded(lower) | Bound::Included(lower) => match upper {
                    Bound::Included(upper) | Bound::Excluded(upper) => {
                        if lower == upper {
                            Some(lower.key_ref())
                        } else {
                            None
                        }
                    }
                    Bound::Unbounded => None,
                },
                Bound::Unbounded => None,
            };

            // 如果不是单个键，返回可能包含
            // 如果是单个键：如果有布隆过滤器，返回布隆过滤器的结果，否则返回可能包含
            single_key.map_or(true, |key| {
                self.bloom
                    .as_ref()
                    .map_or(true, |bloom| bloom.may_contain(SsTable::key_hash(key)))
            })
        }
    }

    pub fn key_hash(key: &[u8]) -> u32 {
        farmhash::fingerprint32(key)
    }
}
