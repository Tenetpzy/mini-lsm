use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FullCompactionTask {
    pub l0_sstables: Vec<usize>,
    pub l1_sstables: Vec<usize>,
}
