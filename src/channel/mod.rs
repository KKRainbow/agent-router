use std::collections::{BTreeSet, VecDeque};

pub(crate) mod context;
pub mod qq;
pub mod slack;

pub(crate) mod output;

#[derive(Debug)]
pub(crate) struct EventDeduper {
    capacity: usize,
    seen: BTreeSet<String>,
    order: VecDeque<String>,
}

impl EventDeduper {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            seen: BTreeSet::new(),
            order: VecDeque::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: String) -> bool {
        if key.is_empty() {
            return true;
        }
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}
