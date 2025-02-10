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

mod full_compact;
mod leveled;
mod simple_leveled;
mod tiered;

use std::fs::remove_file;
use std::mem::transmute;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use full_compact::FullCompactionTask;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction(FullCompactionTask),
}

// impl CompactionTask {
//     fn compact_to_bottom_level(&self) -> bool {
//         match self {
//             CompactionTask::ForceFullCompaction { .. } => true,
//             CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
//             CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
//             CompactionTask::Tiered(task) => task.bottom_tier_included,
//         }
//     }
// }

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

fn get_iter_key<'a, I: StorageIterator<KeyType<'a> = KeySlice<'a>> + 'a>(iter: &I) -> KeySlice<'_> {
    unsafe { transmute(transmute::<&'_ I, &'_ I>(iter).key()) }
}

impl LsmStorageInner {
    /// 'a是无界生命周期，此处避免使用for<'a>限制I导致I: 'static  
    ///
    /// SAFETY:
    /// 1. 此函数不会返回一个'a生命周期的值，所以函数接口是安全的  
    /// 2. 内部实现中，在get_iter_key返回的值存活时，不会再次从iter借用，符合借用栈原则，无UB  
    ///
    /// NOTE：就算需要将返回值传出，也可以使用TwoMergeIterator中的方法，将'a限制到和&self相同，也是安全的
    fn compact_new_sst_from_iter<'a, I: StorageIterator<KeyType<'a> = KeySlice<'a>> + 'a>(
        &self,
        mut iter: I,
        involved_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut result: Vec<Arc<SsTable>> = Vec::new();
        let mut prev_key: Vec<u8> = Vec::new();
        let watermark = self.mvcc().watermark();
        let mut in_keys_below_watermark = false;

        while iter.is_valid() {
            // 只有压实操作包含了最底层时，才能跳过已经删除的键。否则读压实后的SST时可能读到更下层的无效值
            // note: 增加了MVCC后，不能忽略已删除的键，因为可能有旧事务在读
            // if iter.value().is_empty() && involved_bottom_level {
            //     iter.next()?;
            //     continue;
            // }

            let cur_key = get_iter_key(&iter);
            if cur_key.key_ref() == prev_key {
                if in_keys_below_watermark {
                    assert!(cur_key.ts() < watermark);
                    iter.next()?;
                    continue; // 之前保留了一个版本不高于watermark的key，当前的删除
                }
            } else {
                // 遇到不同的key了，重置gc_flag
                in_keys_below_watermark = false;
            }

            // GC: 版本低于watermark的只保留最新版，版本高于watermark的保留
            if cur_key.ts() <= watermark {
                in_keys_below_watermark = true; // 后面遇到的key相同的都删除

                // 如果最新版达到底层并且已经是墓碑，就直接删除(后面的也会删除)
                if iter.value().is_empty() && involved_bottom_level {
                    prev_key.clear();
                    prev_key.extend(cur_key.key_ref());
                    iter.next()?;
                    continue;
                }
            }

            // 保证同一个key的不同版本都在一个SST中，简化其它模块
            if builder.estimated_size() > self.options.target_sst_size
                && cur_key.key_ref() != prev_key
            {
                let sst = self.create_new_sst(builder)?;
                result.push(Arc::new(sst));
                builder = SsTableBuilder::new(self.options.block_size);
            }

            prev_key.clear();
            prev_key.extend(cur_key.key_ref());
            builder.add(cur_key, iter.value());
            iter.next()?;
        }

        if builder.estimated_size() > 0 {
            let sst = self.create_new_sst(builder)?;
            result.push(Arc::new(sst));
        }

        Ok(result)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let result: Vec<Arc<SsTable>>;

        match task {
            CompactionTask::ForceFullCompaction(task) => {
                let mut l0_iters: Vec<Box<SsTableIterator>> = Vec::new();
                for sst_id in &task.l0_sstables {
                    let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(
                        snapshot.get_sst(*sst_id),
                    ))?;
                    l0_iters.push(Box::new(iter));
                }
                let merged_l0_iter = MergeIterator::create(l0_iters);

                let l1_iter = SstConcatIterator::create_and_seek_to_first(
                    task.l1_sstables
                        .iter()
                        .map(|sst_id| Arc::clone(snapshot.get_sst(*sst_id)))
                        .collect(),
                )?;

                let iter = TwoMergeIterator::create(merged_l0_iter, l1_iter)?;
                result = self.compact_new_sst_from_iter(iter, true)?;
                Ok(result)
            }

            CompactionTask::Simple(task) => {
                let lower_iter = SstConcatIterator::create_and_seek_to_first(
                    snapshot.get_ssts(&task.lower_level_sst_ids),
                )?;
                if task.upper_level.is_some() {
                    // 不是L0 compact，iter都可以用concat iterator
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(
                        snapshot.get_ssts(&task.upper_level_sst_ids),
                    )?;
                    let iter =
                        MergeIterator::create(vec![Box::new(upper_iter), Box::new(lower_iter)]);

                    result =
                        self.compact_new_sst_from_iter(iter, task.is_lower_level_bottom_level)?;
                } else {
                    // 是L0 compact, L0只能用merge iterator
                    let mut sst_iters: Vec<Box<SsTableIterator>> = Vec::new();
                    for sst in snapshot.get_ssts(&task.upper_level_sst_ids) {
                        let iter = SsTableIterator::create_and_seek_to_first(sst)?;
                        sst_iters.push(Box::new(iter));
                    }

                    let upper_iter = MergeIterator::create(sst_iters);
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                    result =
                        self.compact_new_sst_from_iter(iter, task.is_lower_level_bottom_level)?;
                }
                Ok(result)
            }

            CompactionTask::Tiered(task) => {
                let mut sst_iter: Vec<Box<SstConcatIterator>> = Vec::new();
                for (_, sst_ids) in &task.tiers {
                    let ssts = snapshot.get_ssts(sst_ids);
                    let iter = SstConcatIterator::create_and_seek_to_first(ssts)?;
                    sst_iter.push(Box::new(iter));
                }
                result = self.compact_new_sst_from_iter(
                    MergeIterator::create(sst_iter),
                    task.bottom_tier_included,
                )?;
                Ok(result)
            }

            CompactionTask::Leveled(task) => {
                // L0: TwoMerge<Merge, Concat>
                if task.upper_level.is_none() {
                    let mut iters: Vec<Box<SsTableIterator>> = Vec::new();
                    for id in &task.upper_level_sst_ids {
                        let iter = SsTableIterator::create_and_seek_to_first(
                            snapshot.get_sst(*id).clone(),
                        )?;
                        iters.push(Box::new(iter));
                    }
                    let upper_iter = MergeIterator::create(iters);

                    let lower_ssts = snapshot.get_ssts(&task.lower_level_sst_ids);
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    result =
                        self.compact_new_sst_from_iter(iter, task.is_lower_level_bottom_level)?;
                } else {
                    // others, Merge<Concat>
                    let upper_ssts = snapshot.get_ssts(&task.upper_level_sst_ids);
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let lower_ssts = snapshot.get_ssts(&task.lower_level_sst_ids);
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                    let iter =
                        MergeIterator::create(vec![Box::new(upper_iter), Box::new(lower_iter)]);
                    result =
                        self.compact_new_sst_from_iter(iter, task.is_lower_level_bottom_level)?;
                }

                Ok(result)
            }
        }
    }

    fn create_new_sst(&self, builder: SsTableBuilder) -> Result<SsTable> {
        let sst_id = self.next_sst_id();
        builder.build(
            sst_id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(sst_id),
        )
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };

        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let compaction_task = CompactionTask::ForceFullCompaction(FullCompactionTask {
            l0_sstables: snapshot.l0_sstables.clone(),
            l1_sstables: snapshot.levels[0].1.clone(),
        });

        let new_sorted_run = self.compact(&compaction_task)?;
        let mut new_sst_ids: Vec<usize> = Vec::with_capacity(new_sorted_run.len());

        // 避免task拷贝，再match出来
        let full_compaction_task = match &compaction_task {
            CompactionTask::ForceFullCompaction(task) => task,
            _ => unreachable!(),
        };

        // TODO: Remove me
        println!(
            "Force full compaction: compact L0:{:?}, L1:{:?} to L1:{:?}",
            full_compaction_task.l0_sstables,
            full_compaction_task.l1_sstables,
            new_sorted_run
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<usize>>()
        );

        let compacted_sst_ids: Vec<usize> = full_compaction_task
            .l0_sstables
            .iter()
            .chain(full_compaction_task.l1_sstables.iter())
            .copied()
            .collect();

        {
            let state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();

            // 在压实操作期间，可能有新的L0_flush发生，保留新的L0索引，去掉已经压实的
            // 由于新的L0只可能在列表头插入，所以直接truncate
            state
                .l0_sstables
                .truncate(state.l0_sstables.len() - full_compaction_task.l0_sstables.len());

            // 清空L1索引
            state.levels[0].1.clear();

            for sst_id in &compacted_sst_ids {
                state.sstables.remove(sst_id);
            }

            for sst in new_sorted_run {
                let id = sst.sst_id();
                new_sst_ids.push(id);
                state.levels[0].1.push(id);
                state.sstables.insert(id, sst);
            }

            *guard = Arc::new(state);

            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(compaction_task, new_sst_ids),
            )?;
        }

        // 删除旧SST文件
        for sst_id in compacted_sst_ids {
            remove_file(self.path_of_sst(sst_id))?;
        }

        self.sync_dir()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = Arc::clone(&self.state.read());
        let compact_task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        if let Some(task) = compact_task {
            println!("trigger compaction, current LSM snapshot:");
            snapshot.dump_structure();

            let new_ssts = self.compact(&task)?;
            let new_sst_ids: Vec<usize> = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            let delete_sst_ids;

            // 更新state时需要上state_lock锁，避免和L0 flush竞争，否则应用compact结果时可能丢失并发刷新的新L0
            {
                let state_lock_guard = self.state_lock.lock();
                let mut snapshot = self.state.read().as_ref().clone();

                // Leveled compaction要求能够在snapshot中查找到新的sst
                for sst in new_ssts {
                    snapshot.sstables.insert(sst.sst_id(), sst);
                }

                let (mut new_state, sst_ids_to_delete) = self
                    .compaction_controller
                    .apply_compaction_result(&snapshot, &task, &new_sst_ids, false);

                // 现在，只需要删除new_state中老的sstable对象，并添加新的sstable对象，最后删除旧对象
                for id in &sst_ids_to_delete {
                    new_state.sstables.remove(id);
                }

                {
                    let mut guard = self.state.write();
                    *guard = Arc::new(new_state);
                }

                // 必须先写manifest，再删SST，否则删完SST系统断电，下次重启时系统还是用上一个快照，但找不到之前的SST了
                self.sync_dir()?;
                self.manifest.as_ref().unwrap().add_record(
                    &state_lock_guard,
                    ManifestRecord::Compaction(task, new_sst_ids),
                )?;

                delete_sst_ids = sst_ids_to_delete;
            }

            // TODO: 写完manifest系统断电，造成残留SST没有被删除，需要GC
            for id in delete_sst_ids {
                remove_file(self.path_of_sst(id))?;
            }

            self.sync_dir()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let state = self.state.read();
        if state.imm_memtables.len() + 1 > self.options.num_memtable_limit {
            drop(state);
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
