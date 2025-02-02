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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::{SstConcatIterator, SstConcatRangeIterator};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SSTRangeIterator, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }

    pub(crate) fn get_sst(&self, sst_id: usize) -> &Arc<SsTable> {
        self.sstables.get(&sst_id).unwrap()
    }

    pub(crate) fn get_ssts(&self, sst_ids: &[usize]) -> Vec<Arc<SsTable>> {
        sst_ids.iter().map(|id| self.get_sst(*id).clone()).collect()
    }

    // *_level, only used in leveled compaction
    pub(crate) fn get_sst_id_in_level(&self, level: usize) -> &Vec<usize> {
        if level == 0 {
            &self.l0_sstables
        } else {
            // levels中索引从0开始，但外层level从1开始
            &self.levels[level - 1].1
        }
    }

    pub(crate) fn get_ssts_in_level_mut(&mut self, level: usize) -> &mut Vec<usize> {
        if level == 0 {
            &mut self.l0_sstables
        } else {
            // levels中索引从0开始，但外层level从1开始
            &mut self.levels[level - 1].1
        }
    }

    /// 丢弃level对应的sstid数组的尾部长度为len_to_trunc_tail的部分
    pub(crate) fn trunc_level(&mut self, level: usize, len_to_trunc_tail: usize) {
        if level == 0 {
            self.l0_sstables
                .truncate(self.l0_sstables.len() - len_to_trunc_tail);
        } else {
            let target = &mut self.levels[level - 1].1;
            target.truncate(target.len() - len_to_trunc_tail);
        }
    }

    pub(crate) fn append_tail_to_level(&mut self, level: usize, new_ssts: &[usize]) {
        if level == 0 {
            self.l0_sstables.extend(new_ssts);
        } else {
            self.levels[level - 1].1.extend(new_ssts);
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        let mut flush_thread_guard = self.flush_thread.lock();
        let flush_thread = flush_thread_guard.take();
        if let Some(flush_thread) = flush_thread {
            self.flush_notifier.send(())?;
            flush_thread.join().unwrap();
        }

        let mut compaction_thread_guard = self.compaction_thread.lock();
        let compaction_thread = compaction_thread_guard.take();
        if let Some(compaction_thread) = compaction_thread {
            self.compaction_notifier.send(())?;
            compaction_thread.join().unwrap();
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
        } else {
            self.inner.freeze_memtable_on_close();
            while {
                let snapshot = self.inner.state.read();
                !snapshot.imm_memtables.is_empty()
            } {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut state = LsmStorageState::create(&options);
        let path = path.as_ref();
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let manifest;

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(x) => {
                        next_sst_id = next_sst_id.max(x);
                        memtables.insert(x);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            let mut sst_cnt = 0;
            // recover SSTs
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .with_context(|| format!("failed to open SST: {}", table_id))?,
                )?;
                state.sstables.insert(table_id, Arc::new(sst));
                sst_cnt += 1;
            }
            println!("{} SSTs opened", sst_cnt);

            next_sst_id += 1;

            // Sort SSTs on each level (only for leveled compaction)
            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_id, ssts) in &mut state.levels {
                    ssts.sort_by(|x, y| {
                        state
                            .sstables
                            .get(x)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(y).unwrap().first_key())
                    })
                }
            }

            // recover memtables
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        if self.options.enable_wal {
            self.state.read().memtable.sync_wal()?;
        }
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // false delete resulting the value's length equals 0.
        let get_value_if_not_deleted = |value: Bytes| {
            if !value.is_empty() {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        };

        let table_may_contain = |sst: &Arc<SsTable>, key: KeySlice| {
            if sst.key_within(key) {
                // key在范围内，再用布隆过滤器过一遍
                if let Some(bloom) = &sst.bloom {
                    bloom.may_contain(SsTable::key_hash(key.raw_ref()))
                } else {
                    true
                }
            } else {
                false
            }
        };

        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };
        match snapshot.memtable.get(key) {
            Some(value) => return get_value_if_not_deleted(value),
            None => {
                for imm_table in &snapshot.imm_memtables {
                    match imm_table.get(key) {
                        Some(value) => return get_value_if_not_deleted(value),
                        None => continue,
                    }
                }
            }
        };

        let key = KeySlice::from_slice(key);
        for sst_id in &snapshot.l0_sstables {
            let sst = snapshot.get_sst(*sst_id);

            if table_may_contain(sst, key) {
                let iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;

                // 性能有问题！需要拷贝value值到新的Bytes中
                if iter.is_valid() && iter.key() == key {
                    return get_value_if_not_deleted(Bytes::copy_from_slice(iter.value()));
                }
            }
        }

        for (_, sst_ids) in &snapshot.levels {
            let mut ssts: Vec<Arc<SsTable>> = Vec::new();
            for id in sst_ids {
                let sst = snapshot.get_sst(*id);
                if table_may_contain(sst, key) {
                    ssts.push(sst.clone());
                }
            }

            let iter = SstConcatIterator::create_and_seek_to_key(ssts, key)?;
            if iter.is_valid() && iter.key() == key {
                return get_value_if_not_deleted(Bytes::copy_from_slice(iter.value()));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// check approximate size and freeze if needed. Caller cannot holds state or state_lock.
    fn try_freeze(&self, approximate_size: usize) -> Result<()> {
        if approximate_size > self.options.target_sst_size {
            let guard = self.state_lock.lock();

            // Now we need to check again. Because try_freeze is called concurrently.
            // Cannot avoid to hold read lock again. Because lock sequence state_lock > state, preventing deadlock.
            let approximate_size = self.state.read().memtable.approximate_size();
            if approximate_size > self.options.target_sst_size {
                // current thread gains the right to freeze
                self.force_freeze_memtable(&guard)?;
            }
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let approximate_size: usize;
        {
            let state = self.state.read();
            state.memtable.put(key, value)?;
            approximate_size = state.memtable.approximate_size();
        }
        self.try_freeze(approximate_size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        // self.state.read().memtable.put(key, &[])
        // for delete, we shouldn't but still need to add the approximate_size because test logical.
        let approximate_size: usize;
        {
            let state = self.state.read();
            state.memtable.put(key, b"")?;
            approximate_size = state.memtable.approximate_size();
        }
        self.try_freeze(approximate_size)?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)
            .context("failed to open LSM dir")?
            .sync_all()
            .context("failed to sync LSM dir")
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_table_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            let memtable = MemTable::create_with_wal(new_table_id, self.path_of_wal(new_table_id))?;
            Arc::new(memtable)
        } else {
            Arc::new(MemTable::create(new_table_id))
        };

        // holds write lock now minimizing critical section
        let old_memtable = {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            let old_memtable = Arc::clone(&snapshot.memtable);
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            snapshot.memtable = new_memtable;
            *state = Arc::new(snapshot);
            old_memtable
        };

        old_memtable.sync_wal()?;
        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(new_table_id),
        )?;

        Ok(())
    }

    fn freeze_memtable_on_close(&self) {
        let mut state = self.state.write();
        if state.memtable.is_empty() {
            return;
        }
        let mut snapshot = state.as_ref().clone();
        let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create(0)));
        snapshot.imm_memtables.insert(0, memtable);
        *state = Arc::new(snapshot);
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let guard = self.state_lock.lock();

        let target_imm_memtable = {
            let state = self.state.read();
            match state.imm_memtables.last().map(Arc::clone) {
                Some(imm_table) => imm_table,
                None => return Ok(()), // 可能因为用户也主动进行了flush，所以没有immtable不算错误
            }
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        target_imm_memtable.flush(&mut sst_builder)?;
        let sst_id = target_imm_memtable.id();
        let sst = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            snapshot.sstables.insert(sst_id, Arc::new(sst));
            *state = Arc::new(snapshot);
        }

        self.sync_dir()?; // 先同步一次目录，保证目录里新的SST都可见，再写manifest文件
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&guard, ManifestRecord::Flush(sst_id))?;

        // 必须写完manifest，再删除immtable的WAL。否则WAL删了，断电，manifest没有写入，
        // 下次启动时，系统仍然使用上一个manifest快照，认为该immtable还没刷到SST，但找不到WAL，就无法恢复该immtable了
        // TODO: 如果写完manifest，没删WAL断电，系统里会残留无用的WAL文件
        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut mem_iters: Vec<Box<MemTableIterator>> = Vec::new();
        mem_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for imm_tables in &snapshot.imm_memtables {
            mem_iters.push(Box::new(imm_tables.scan(lower, upper)));
        }

        let mut l0_sst_iters: Vec<Box<SSTRangeIterator>> = Vec::new();
        for sst_idx in &snapshot.l0_sstables {
            let sst = snapshot.get_sst(*sst_idx);

            if sst.range_overlap(lower, upper) {
                let iter = SSTRangeIterator::create(Arc::clone(sst), lower, upper)?;
                l0_sst_iters.push(Box::new(iter));
            }
        }

        let mut sorted_runs_iter: Vec<Box<SstConcatRangeIterator>> = Vec::new();
        for (_, sst_id_vec) in &snapshot.levels {
            let mut ssts: Vec<Arc<SsTable>> = Vec::new();
            for sst_id in sst_id_vec {
                let sst = snapshot.get_sst(*sst_id);
                if sst.range_overlap(lower, upper) {
                    ssts.push(sst.clone());
                }
            }

            let iter = SstConcatRangeIterator::create(ssts, lower, upper)?;
            sorted_runs_iter.push(Box::new(iter));
        }

        let iter = TwoMergeIterator::create(
            MergeIterator::create(mem_iters),
            TwoMergeIterator::create(
                MergeIterator::create(l0_sst_iters),
                MergeIterator::create(sorted_runs_iter),
            )?,
        )?;
        LsmIterator::new(iter).map(FusedIterator::new)
    }
}
