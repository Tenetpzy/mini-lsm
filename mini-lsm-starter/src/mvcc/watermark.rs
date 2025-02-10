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

use std::collections::BTreeMap;

/// 记录当前活跃的事务：<读时间戳，拥有此读时间戳的事务个数>
/// note: 事务开始时，使用当前的提交时间戳作为读时间戳，而不是将全局时间戳+1，避免了一些开销，
/// 但是存在多个事务使用同一个读时间戳的问题，所以这里要记录个数
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        let count = self.readers.get_mut(&ts);
        match count {
            Some(count) => *count += 1,
            None => {
                self.readers.insert(ts, 1);
            }
        };
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let count = self.readers.get_mut(&ts).unwrap();
        *count -= 1;
        if *count == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    /// 返回系统中当前最小的read_ts
    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(&ts, _)| ts)
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}
