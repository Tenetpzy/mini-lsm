use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut task: Option<SimpleLeveledCompactionTask> = None;
        if self.need_l0_compact(snapshot) {
            task = Some(self.generate_l0_compact_task(snapshot));
        } else {
            for level in 1..self.options.max_levels {
                if self.need_level_compact(snapshot, level) {
                    task = Some(self.generate_level_compact_task(snapshot, level));
                    break;
                }
            }
        }
        task
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    ///
    /// 注意：只apply l0_sstables和levels，不修改sstables
    /// output是新生成的SST id列表，返回值中的Vec<usize>是要删除的SST id列表
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let upper_level = task.upper_level.unwrap_or(0);
        let lower_level = upper_level + 1;

        println!(
            "In SimpleLeveledCompaction apply_compaction_result, compact level {} to level {}:",
            upper_level, lower_level
        );
        println!("Before compaction:");
        snapshot.dump_structure();

        snapshot.trunc_level(upper_level, task.upper_level_sst_ids.len());
        snapshot.trunc_level(lower_level, task.lower_level_sst_ids.len());
        snapshot.append_tail_to_level(lower_level, output);

        println!("After compaction:");
        snapshot.dump_structure();

        (
            snapshot,
            task.lower_level_sst_ids
                .iter()
                .chain(task.upper_level_sst_ids.iter())
                .copied()
                .collect(),
        )
    }

    fn need_l0_compact(&self, snapshot: &LsmStorageState) -> bool {
        if (snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger)
            && self.need_level_compact(snapshot, 0)
        {
            true
        } else {
            false
        }
    }

    // upper_level 和 upper_level + 1 是否需要compaction
    fn need_level_compact(&self, snapshot: &LsmStorageState, upper_level: usize) -> bool {
        let upper_len = snapshot.get_sst_id_in_level(upper_level).len();
        let lower_len = snapshot.get_sst_id_in_level(upper_level + 1).len();

        if upper_len == 0 {
            false
        } else if lower_len / upper_len * 100 < self.options.size_ratio_percent {
            true
        } else {
            false
        }
    }

    fn generate_l0_compact_task(&self, snapshot: &LsmStorageState) -> SimpleLeveledCompactionTask {
        SimpleLeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids: snapshot.get_sst_id_in_level(0).clone(),
            lower_level: 1,
            lower_level_sst_ids: snapshot.get_sst_id_in_level(1).clone(),
            is_lower_level_bottom_level: self.options.max_levels == 1,
        }
    }

    fn generate_level_compact_task(
        &self,
        snapshot: &LsmStorageState,
        level: usize,
    ) -> SimpleLeveledCompactionTask {
        SimpleLeveledCompactionTask {
            upper_level: Some(level),
            upper_level_sst_ids: snapshot.get_sst_id_in_level(level).clone(),
            lower_level: level + 1,
            lower_level_sst_ids: snapshot.get_sst_id_in_level(level + 1).clone(),
            is_lower_level_bottom_level: self.options.max_levels == level + 1,
        }
    }
}
