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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

// level从1开始
#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap_ssts = Vec::new();
        for sst_id in snapshot.get_sst_id_in_level(in_level) {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts

        // 我的解法：对每个上层SST，寻找下层与他有交集的，最后并起来
        // 能够运行压缩模拟器的解法：取上层SST所有范围构成的最左端和最右端，找下层中和这个范围有交集的（压缩模拟器默认这个假设）
        // 但是这个解法可能产生额外的写放大：如果上层SST范围之间有空洞，而这些空洞在下层有实际SST，那么这些SST其实不用重写的
        // 为了能够测试，采用项目提供的做法
        // let mut res: HashSet<usize> = HashSet::new();
        // let sst_in_level = snapshot.get_sst_id_in_level(in_level);

        // for id in sst_ids {
        //     let sst = snapshot.get_sst(*id);
        //     let targets = sst_in_level
        //         .iter()
        //         .filter_map(|&id| {
        //             let target = snapshot.get_sst(id);
        //             let overlap = target.range_overlap(
        //                 Bound::Included(sst.first_key().raw_ref()),
        //                 Bound::Included(sst.last_key().raw_ref()),
        //             );
        //             if overlap {
        //                 Some(id)
        //             } else {
        //                 None
        //             }
        //         })
        //         .into_iter();
        //     res.extend(targets);
        // }

        // Vec::from_iter(res.into_iter())
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_cur_size = Self::levels_cur_size(snapshot);
        let level_target_size =
            self.levels_target_size(level_cur_size[self.options.max_levels - 1]);
        assert!(level_cur_size.len() == level_target_size.len());

        // 优先压缩L0
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let lower_level = level_cur_size
                .iter()
                .zip(level_target_size.iter())
                .position(|(&cur_size, &tar_size)| cur_size != 0 || tar_size != 0)
                .map_or(self.options.max_levels, |level| level + 1);

            let upper_level_sst_ids = snapshot.get_sst_id_in_level(0).clone();
            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &upper_level_sst_ids, lower_level);

            let task = LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: lower_level == self.options.max_levels,
            };

            println!(
                "generate leveled compaction task: compact L0:{:?} to L{}:{:?}",
                task.upper_level_sst_ids, lower_level, task.lower_level_sst_ids
            );

            return Some(task);
        }

        // 计算具有最高压缩比的层
        let (level, ratio) = level_cur_size
            .iter()
            .zip(level_target_size.iter())
            .map(|(&cur_size, &tar_size)| {
                if tar_size == 0 {
                    if cur_size == 0 {
                        0.0
                    } else {
                        f64::MAX
                    }
                } else {
                    cur_size as f64 / tar_size as f64
                }
            })
            .enumerate()
            .reduce(|(cur_max_level, cur_max), (level, ratio)| {
                if cur_max.max(ratio) == ratio {
                    (level, ratio)
                } else {
                    (cur_max_level, cur_max)
                }
            })
            .unwrap();

        if ratio > 1.0 {
            let upper_level = level + 1;

            // 选择最老的SST
            let upper_sst_id = *snapshot
                .get_sst_id_in_level(upper_level)
                .iter()
                .min()
                .unwrap();
            let upper_level_sst_ids = vec![upper_sst_id];
            let lower_level = upper_level + 1;
            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &upper_level_sst_ids, lower_level);

            let task = LeveledCompactionTask {
                upper_level: Some(upper_level),
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: lower_level == self.options.max_levels,
            };

            println!(
                "generate leveled compaction task: compact L{}:{:?} to L{}:{:?}\n\
                L{} current size = {:?}MB, target size = {:?}MB, compaction ratio = {:?}",
                upper_level,
                task.upper_level_sst_ids,
                lower_level,
                task.lower_level_sst_ids,
                upper_level,
                level_cur_size[level] as f64 / (1024.0 * 1024.0),
                level_target_size[level] as f64 / (1024.0 * 1024.0),
                ratio
            );

            return Some(task);
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let upper_level = task.upper_level.unwrap_or(0);
        let lower_level = task.lower_level;

        println!(
            "In leveled apply_compaction_result, compact L{}:{:?}, L{}:{:?} to L{}:{:?}:",
            upper_level,
            task.upper_level_sst_ids,
            lower_level,
            task.lower_level_sst_ids,
            lower_level,
            output
        );

        println!("Before compaction:");
        snapshot.dump_structure();

        let mut upper_level_delete_set: HashSet<usize> =
            task.upper_level_sst_ids.iter().copied().collect();
        let new_upper_level: Vec<usize> = snapshot
            .get_sst_id_in_level(upper_level)
            .iter()
            .copied()
            .filter(|id| !upper_level_delete_set.remove(id))
            .collect();
        assert!(upper_level_delete_set.is_empty());
        *snapshot.get_ssts_in_level_mut(upper_level) = new_upper_level;

        let mut lower_level_delete_set: HashSet<usize> =
            task.lower_level_sst_ids.iter().copied().collect();
        let mut new_lower_level: Vec<usize> = snapshot
            .get_sst_id_in_level(lower_level)
            .iter()
            .copied()
            .filter(|id| !lower_level_delete_set.remove(id))
            .chain(output.iter().copied())
            .collect();
        assert!(lower_level_delete_set.is_empty());

        // 在启动的恢复阶段，snapshot里没有打开SST，所以这里不能排序，要等待恢复完成后再排
        if !in_recovery {
            new_lower_level.sort_by(|a, b| {
                let sst_a = snapshot.get_sst(*a);
                let sst_b = snapshot.get_sst(*b);
                sst_a.first_key().cmp(sst_b.first_key())
            });
        }

        *snapshot.get_ssts_in_level_mut(lower_level) = new_lower_level;

        println!("After compaction:");
        snapshot.dump_structure();

        (
            snapshot,
            [
                task.upper_level_sst_ids.clone(),
                task.lower_level_sst_ids.clone(),
            ]
            .concat(),
        )
    }

    fn levels_cur_size(snapshot: &LsmStorageState) -> Vec<usize> {
        snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                snapshot
                    .get_ssts(sst_ids)
                    .iter()
                    .map(|sst| sst.table_size() as usize)
                    .sum()
            })
            .collect()
    }

    fn levels_target_size(&self, bottom_level_cur_size: usize) -> Vec<usize> {
        let level_num = self.options.max_levels;
        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;
        let mut level_sizes: Vec<usize> = vec![0; level_num];

        level_sizes[level_num - 1] = bottom_level_cur_size;
        for i in (0..level_num - 1).rev() {
            if level_sizes[i + 1] <= base_level_size {
                level_sizes[i] = 0;
            } else {
                level_sizes[i] = level_sizes[i + 1] / self.options.level_size_multiplier;
            }
        }

        level_sizes
    }

    fn level_cur_size(snapshot: &LsmStorageState, level: usize) -> usize {
        snapshot.levels[level]
            .1
            .iter()
            .map(|sst_id| snapshot.get_sst(*sst_id).table_size() as usize)
            .sum()
    }
}
