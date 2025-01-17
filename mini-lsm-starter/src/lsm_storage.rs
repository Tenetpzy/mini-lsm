#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
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
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SSTRangeIterator, SsTable, SsTableBuilder, SsTableIterator};

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

    pub(crate) fn get_sst_id_in_level(&self, level: usize) -> &Vec<usize> {
        if level == 0 {
            &self.l0_sstables
        } else {
            // levels中索引从0开始，但外层level从1开始
            &self.levels[level - 1].1
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
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

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

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
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

        // TODO: 需要确保所有压实策略中level数组下标小的SST是最新的
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
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        // holds write lock now minimizing critical section
        let mut state = self.state.write();
        let mut snapshot = state.as_ref().clone();
        let old_memtable = Arc::clone(&snapshot.memtable);
        snapshot.imm_memtables.insert(0, old_memtable);
        snapshot.memtable = new_memtable;
        *state = Arc::new(snapshot);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _guard = self.state_lock.lock();

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
            snapshot.l0_sstables.insert(0, sst_id);
            snapshot.sstables.insert(sst_id, Arc::new(sst));
            *state = Arc::new(snapshot);
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
