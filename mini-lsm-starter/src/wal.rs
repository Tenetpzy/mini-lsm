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

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            let batch_size = rbuf.get_u32() as usize;
            let mut batch_readed = 0_usize;
            let mut hasher = crc32fast::Hasher::new();
            while batch_readed != batch_size {
                let key_len = rbuf.get_u16() as usize;
                hasher.write_u16(key_len as u16);
                let key = Bytes::copy_from_slice(&rbuf[..key_len]);
                hasher.write(&key);
                rbuf.advance(key_len);
                let ts = rbuf.get_u64();
                hasher.write_u64(ts);
                let value_len = rbuf.get_u16() as usize;
                hasher.write_u16(value_len as u16);
                let value = Bytes::copy_from_slice(&rbuf[..value_len]);
                hasher.write(&value);
                rbuf.advance(value_len);

                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
                batch_readed +=
                    size_of::<u16>() + key_len + size_of::<u64>() + size_of::<u16>() + value_len;
            }
            let checksum = rbuf.get_u32();
            if hasher.finalize() != checksum {
                bail!("checksum mismatch");
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    /// 事务提交写入的一批数据的WAL，要求原子写入
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let batch_size: usize = data
            .iter()
            .map(|(k, v)| {
                // kv: key_len(u16) | key | ts | value_len(u16) | value
                k.raw_len() + v.len() + std::mem::size_of::<u16>() * 2
            })
            .sum();

        // batch_size(u32) | kv1 | kv2 | ... | kvn | checksum(u32)
        let mut buf: Vec<u8> = Vec::with_capacity(batch_size + std::mem::size_of::<u32>() * 2);
        buf.put_u32(batch_size as u32);
        let mut hasher = crc32fast::Hasher::new();

        for (key, value) in data {
            hasher.write_u16(key.key_len() as u16);
            buf.put_u16(key.key_len() as u16);
            hasher.write(key.key_ref());
            buf.put_slice(key.key_ref());
            hasher.write_u64(key.ts());
            buf.put_u64(key.ts());
            hasher.write_u16(value.len() as u16);
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
            hasher.write(value);
        }

        // add checksum
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
