use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // compaction triggered by space amplification ratio
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // compaction triggered by size ratio
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / size as f64;
            if current_size_ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    current_size_ratio * 100.0,
                    size_ratio_trigger * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(id + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: id + 1 >= snapshot.levels.len(),
                });
            }
        }

        // trying to reduce sorted runs without respecting size ratio
        let num_tiers_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        Self::dump_compact_info(task, output);
        let mut snapshot = snapshot.clone();
        println!("before compaction:");
        snapshot.dump_structure();

        // compaction过程中可能会发生新的flush，且未来可能会并发compaction
        // 无法通过levels中的下标定位移除的tiers(leveled compaction中可以是因为flush刷到L0, 不影响levels)
        // 所以这里遍历snapshot.levels，通过hash查找，定位在当前compaction中移除的tiers
        // 并且将新的tier插入到被移除的这些tiers的紧接的下层，不影响其他compaction

        let mut compacted_tiers: HashMap<usize, &Vec<usize>> =
            task.tiers.iter().map(|(id, level)| (*id, level)).collect();
        let mut new_tier_inserted = false;
        let mut new_levels: Vec<(usize, Vec<usize>)> = Vec::new();
        let mut sst_to_delete: Vec<usize> = Vec::new();

        for (id, sstids) in snapshot.levels {
            if let Some(ssts) = compacted_tiers.remove(&id) {
                // 这一层被compaction了，需要移除
                assert_eq!(
                    *ssts, sstids,
                    "sst lists are not equal in snapshot and tiered compaction task in tier {}",
                    id
                );
                sst_to_delete.extend(ssts);
            } else {
                // 这一层没有被compaction，保留
                new_levels.push((id, sstids));
            }

            if compacted_tiers.is_empty() && !new_tier_inserted {
                // 全部受compaction影响的tiers移除完了，立刻插入新tier
                new_tier_inserted = true;
                new_levels.push((output[0], output.to_vec()));
            }
        }

        assert!(compacted_tiers.is_empty(), "snapshot missing tiers?");

        snapshot.levels = new_levels;

        println!("after compaction:");
        snapshot.dump_structure();

        (snapshot, sst_to_delete)
    }

    fn dump_compact_info(task: &TieredCompactionTask, result: &[usize]) {
        println!("tiered compaction:");
        for (id, sstids) in &task.tiers {
            print!("L{}: {:?} ", *id, *sstids);
        }
        println!("-> L{}: {:?}", result[0], result);
    }
}
