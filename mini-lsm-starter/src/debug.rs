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
