use std::mem::transmute;

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    cur: CurrentIter,
}

// 正常的设计应该加一个Invalid表示A和B都无效
// 但是这样要求KeyType实现default，否则Invalid时key方法无法返回
enum CurrentIter {
    A,
    B,
}

impl<'a, A: StorageIterator + 'a, B: StorageIterator<KeyType<'a> = A::KeyType<'a>> + 'a>
    TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            cur: CurrentIter::A,
        };
        iter.update_current_iter();
        Ok(iter)
    }

    // 将cur指向A和B中当前key最小的一个
    fn update_current_iter(&mut self) {
        if self.a.is_valid() && self.b.is_valid() {
            self.cur = if self.transmute().a.key() <= self.transmute().b.key() {
                CurrentIter::A
            } else {
                CurrentIter::B
            };
        } else if self.a.is_valid() {
            self.cur = CurrentIter::A;
        } else if self.b.is_valid() {
            self.cur = CurrentIter::B;
        }
    }

    fn transmute<'b>(&self) -> &'b Self {
        unsafe { std::mem::transmute(self) }
    }
}

/// NOTE: 原来的实现中，要求A: 'static, B: 'static，A和B都不能有生命周期参数  
/// 但是迭代器本身拥有生命周期是很正常的事，只不过这个项目里使用Arc<Memtable>避免了  
/// 为了更加通用，这里使用一个'a，去掉'static  
///
/// **SAFETY:**  
/// 'a看上去不受任何约束，是无界生命周期，但是它在对外的key接口中被合理的限制了  
/// 内部的transmute操作只是取悦编译器，让它无视这个'a的约束，否则borrow checker不理解'a  
/// 我们只用确保内部满足借用栈模型就没有UB，即transmute返回值销毁前，不能再次对self借用
impl<'a, A: StorageIterator + 'a, B: StorageIterator<KeyType<'a> = A::KeyType<'a>> + 'a>
    StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'b>
        = A::KeyType<'b>
    where
        Self: 'b;

    fn key(&self) -> Self::KeyType<'_> {
        match self.cur {
            CurrentIter::A => self.a.key(),
            CurrentIter::B => unsafe {
                transmute::<<A as StorageIterator>::KeyType<'a>, <A as StorageIterator>::KeyType<'_>>(
                    self.transmute().b.key(),
                )
            },
        }
    }

    fn value(&self) -> &[u8] {
        match self.cur {
            CurrentIter::A => self.a.value(),
            CurrentIter::B => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        match self.cur {
            CurrentIter::A => {
                // 当B有与A相同的Key时，优先使用A并忽略B
                while self.b.is_valid() && self.transmute().b.key() == self.transmute().key() {
                    self.b.next()?;
                }
                self.a.next()?;
            }
            CurrentIter::B => {
                // 此时B不可能有与A相同的Key，否则cur应指向A，A不用做next操作
                self.b.next()?;
            }
        }
        self.update_current_iter();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
