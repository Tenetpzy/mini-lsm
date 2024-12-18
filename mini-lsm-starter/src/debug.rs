use crate::lsm_storage::{LsmStorageInner, LsmStorageState, MiniLsm};

impl LsmStorageState {
    pub fn dump_structure(&self) {
        if !self.l0_sstables.is_empty() {
            println!("L0 ({}): {:?}", self.l0_sstables.len(), self.l0_sstables,);
        }
        for (level, files) in &self.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        snapshot.dump_structure();
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
