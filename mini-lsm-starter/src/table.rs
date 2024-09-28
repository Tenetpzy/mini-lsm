#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::{IoSlice, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::Buf;
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
            offset, first_key, last_key
        }
    }

    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.  
    /// encode format:  
    /// BlockMeta_number (le u64) | BlockMeta | ... | BlockMeta
    /// each BlockMeta: offset(le u64) | first_key_len(le u64) | first_key | last_key_len(le u64) | last_key
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.extend((block_meta.len() as u64).to_le_bytes());
        for meta in block_meta {
            buf.extend((meta.offset as u64).to_le_bytes());
            buf.extend((meta.first_key.len() as u64).to_le_bytes());
            buf.extend(meta.first_key.raw_ref());
            buf.extend((meta.last_key.len() as u64).to_le_bytes());
            buf.extend(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let cnt = buf.get_u64_le();
        let mut res: Vec<BlockMeta> = Vec::new();
        
        for _ in 0..cnt {
            let offset = buf.get_u64_le();
            let first_key_len = buf.get_u64_le();
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len as usize));
            let last_key_len = buf.get_u64_le();
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len as usize));

            res.push(BlockMeta::new(offset as usize, first_key, last_key));
        }

        res
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
        file.write_vectored(datas)?;
        file.sync_all()?;
        let len = file.metadata()?.len();
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            len
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
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
        
    }

    pub fn create(
        file: FileObject,
        block_meta: Vec<BlockMeta>,
        block_meta_offset: usize,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        first_key: KeyBytes,
        last_key: KeyBytes,
        bloom: Option<Bloom>,
        max_ts: u64
    ) -> Self {
        Self { file, block_meta, block_meta_offset, id, block_cache, first_key, last_key, bloom, max_ts }
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
            block_meta_offset: 0,
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
        unimplemented!()
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        unimplemented!()
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

    fn decode_block_meta_offset(&self) -> Result<usize> {
        let encode_offset: [u8; 4] = 
            self.file.read(self.file.size() - 4, 4)?
            .try_into()
            .map_err(|_| anyhow!("failed to transfer encode_offset: Vec<u8> -> [u8; 4]"))?;

        Ok(u32::from_le_bytes(encode_offset) as usize)
    }
}
