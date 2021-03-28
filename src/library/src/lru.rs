/// Tracks least recently used items.
pub struct LruVec<T> {
    capacity: usize,
    entries: Vec<(u64, T)>,
    counter: u64,
}

impl<T> LruVec<T>
where
    T: PartialEq,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::with_capacity(capacity),
            counter: u64::MAX,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Add an item or update an existing item to the front.
    ///
    /// Returns an evicted item if any.
    pub fn insert(&mut self, item: T) -> Option<T> {
        if self.find_and_update(&item) {
            self.sort_items();
            None
        } else if self.entries.len() == self.capacity {
            let old_entry = self.entries.pop();
            self.counter -= 1;
            self.entries.insert(0, (self.counter, item));
            debug_assert!(self.entries.len() <= self.capacity);

            match old_entry {
                Some(item) => Some(item.1),
                None => None,
            }
        } else {
            self.counter -= 1;
            self.entries.insert(0, (self.counter, item));
            debug_assert!(self.entries.len() <= self.capacity);
            None
        }
    }

    /// Move an item to the front.
    ///
    /// Returns whether the item exists.
    pub fn touch(&mut self, item: &T) -> bool {
        if self.find_and_update(item) {
            self.sort_items();
            true
        } else {
            false
        }
    }

    /// Remove all items and returns them.
    #[allow(dead_code)]
    pub fn clear(&mut self) -> Vec<T> {
        let mut new_vec = Vec::with_capacity(self.entries.len());

        while let Some(entry) = self.entries.pop() {
            new_vec.push(entry.1);
        }

        new_vec.reverse();

        new_vec
    }

    fn find_and_update(&mut self, item: &T) -> bool {
        for current_item in self.entries.iter_mut() {
            if &current_item.1 == item {
                self.counter -= 1;
                current_item.0 = self.counter;
                return true;
            }
        }

        false
    }

    fn sort_items(&mut self) {
        self.entries.sort_unstable_by_key(|item| item.0);
    }

    #[cfg(test)]
    pub(in crate::lru) fn item_at(&self, index: usize) -> Option<&T> {
        let entry = self.entries.get(index)?;
        Some(&entry.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_vec() {
        let mut lru = LruVec::<u32>::new(3);

        assert!(!lru.touch(&1));

        assert!(lru.insert(1).is_none()); // [1]
        assert!(lru.insert(2).is_none()); // [2, 1]

        assert_eq!(lru.len(), 2);
        assert!(!lru.is_empty());

        assert!(lru.insert(3).is_none()); // [3, 2, 1]
        assert_eq!(lru.insert(4), Some(1)); // [4, 3, 2]

        assert_eq!(lru.item_at(0), Some(&4));
        assert_eq!(lru.item_at(1), Some(&3));
        assert_eq!(lru.item_at(2), Some(&2));

        assert!(lru.touch(&3)); // [3, 4, 2]

        assert_eq!(lru.item_at(0), Some(&3));
        assert_eq!(lru.item_at(1), Some(&4));
        assert_eq!(lru.item_at(2), Some(&2));

        let items = lru.clear();
        assert_eq!(&items, &[3, 4, 2]);
    }
}
