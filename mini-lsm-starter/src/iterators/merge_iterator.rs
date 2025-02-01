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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use super::StorageIterator;
use anyhow::Result;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    // 始终保证堆内所有迭代器都是有效的
    iters: BinaryHeap<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut merge_iter: MergeIterator<I> = MergeIterator {
            iters: BinaryHeap::new(),
        };

        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                merge_iter.iters.push(HeapWrapper(index, iter));
            }
        }

        merge_iter
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn key(&self) -> Self::KeyType<'_> {
        self.iters.peek().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.iters.peek().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.iters.peek().is_some()
    }

    fn next(&mut self) -> Result<()> {
        // 先移除当前堆顶，rust引用机制不允许在获得堆顶可变引用的同时修改堆中其它元素
        if let Some(mut cur_iter) = self.iters.pop() {
            // step1: 将每一个迭代器中与当前top迭代器的key相同的元素移除（只返回最新的key）
            while let Some(mut top) = self.iters.peek_mut() {
                if top.1.key() == cur_iter.1.key() {
                    // 如果迭代器next导致内部出错
                    // 直接移除该迭代器并返回错误，否则peekmut调整堆时会继续访问迭代器导致panic
                    if let Err(e) = top.1.next() {
                        PeekMut::pop(top);
                        return Err(e);
                    }
                    // 如果迭代器已经迭代完了
                    else if !top.1.is_valid() {
                        PeekMut::pop(top);
                    }
                } else {
                    break;
                }
            }

            // step2: 将当前迭代器后移并重新插入堆
            cur_iter.1.next()?;
            if cur_iter.1.is_valid() {
                self.iters.push(cur_iter);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let mut res = 0usize;
        for iter in &self.iters {
            res += iter.1.num_active_iterators();
        }
        res
    }
}
