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

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
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
            self.cur = if self.a.key() <= self.b.key() {
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
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.cur {
            CurrentIter::A => self.a.key(),
            CurrentIter::B => self.b.key(),
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
                while self.b.is_valid() && self.b.key() == self.key() {
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
}
