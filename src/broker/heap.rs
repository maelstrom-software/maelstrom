#![allow(dead_code)]

use std::cmp::Ordering;

pub trait HeapDeps {
    type Element: Copy;
    fn compare_elements(&self, lhs: Self::Element, rhs: Self::Element) -> Ordering;
    fn notify_of_index_change(&self, elem: Self::Element, idx: usize);
}

pub struct Heap<D: HeapDeps> {
    deps: D,
    vec: Vec<D::Element>,
}

impl<D: HeapDeps> Heap<D> {
    pub fn new(deps: D) -> Heap<D> {
        Heap { deps, vec: vec![] }
    }

    pub fn peek(&self) -> Option<D::Element> {
        if self.vec.is_empty() {
            None
        } else {
            Some(self.vec[0])
        }
    }

    pub fn push(&mut self, elem: D::Element) {
        let idx = self.vec.len();
        self.vec.push(elem);
        let idx = self.up_heap_internal(idx);
        self.deps.notify_of_index_change(elem, idx);
    }

    pub fn remove(&mut self, idx: usize) {
        self.vec.swap_remove(idx);
        if idx < self.vec.len() {
            let mut new_idx = self.up_heap_internal(idx);
            if new_idx == idx {
                new_idx = self.down_heap_internal(idx);
            }
            self.deps.notify_of_index_change(self.vec[new_idx], new_idx);
        }
    }

    pub fn up_heap(&mut self, idx: usize) {
        let new_idx = self.up_heap_internal(idx);
        if new_idx != idx {
            self.deps.notify_of_index_change(self.vec[new_idx], new_idx);
        }
    }

    pub fn down_heap(&mut self, idx: usize) {
        let new_idx = self.down_heap_internal(idx);
        if new_idx != idx {
            self.deps.notify_of_index_change(self.vec[new_idx], new_idx);
        }
    }

    fn parent(idx: usize) -> usize {
        (idx + 1) / 2 - 1
    }

    fn left_child(idx: usize) -> usize {
        (idx + 1) * 2 - 1
    }

    fn up_heap_internal(&mut self, mut idx: usize) -> usize {
        while idx != 0 {
            let parent_idx = Self::parent(idx);

            if self
                .deps
                .compare_elements(self.vec[parent_idx], self.vec[idx])
                != Ordering::Greater
            {
                break;
            }

            self.vec.swap(parent_idx, idx);
            self.deps.notify_of_index_change(self.vec[idx], idx);
            idx = parent_idx;
        }
        idx
    }

    fn down_heap_internal(&mut self, mut idx: usize) -> usize {
        loop {
            let mut child_idx = Self::left_child(idx);
            if child_idx >= self.vec.len() {
                break;
            }
            if child_idx + 1 < self.vec.len()
                && self
                    .deps
                    .compare_elements(self.vec[child_idx], self.vec[child_idx + 1])
                    != Ordering::Less
            {
                child_idx = child_idx + 1;
            }

            if self
                .deps
                .compare_elements(self.vec[child_idx], self.vec[idx])
                != Ordering::Less
            {
                break;
            }

            self.vec.swap(child_idx, idx);
            self.deps.notify_of_index_change(self.vec[idx], idx);
            idx = child_idx;
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    struct FakeState {
        indices: HashMap<i32, usize>,
    }

    impl FakeState {
        fn new() -> Self {
            FakeState {
                indices: HashMap::new(),
            }
        }
    }

    impl HeapDeps for Rc<RefCell<FakeState>> {
        type Element = i32;

        fn compare_elements(&self, lhs: i32, rhs: i32) -> Ordering {
            lhs.cmp(&rhs)
        }

        fn notify_of_index_change(&self, elem: i32, idx: usize) {
            self.borrow_mut().indices.insert(elem, idx);
        }
    }

    struct Fixture {
        fake_state: Rc<RefCell<FakeState>>,
        heap: Heap<Rc<RefCell<FakeState>>>,
    }

    impl Fixture {
        fn new() -> Self {
            let fake_state = Rc::new(RefCell::new(FakeState::new()));
            let heap = Heap::new(fake_state.clone());
            Fixture { fake_state, heap }
        }

        fn assert_index(&self, val: i32, expected: usize) {
            assert_eq!(
                *self.fake_state.borrow().indices.get(&val).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn foo() {
        let mut fixture = Fixture::new();
        fixture.heap.push(1);
        fixture.assert_index(1, 0);

        fixture.heap.push(2);
        fixture.assert_index(1, 0);
        fixture.assert_index(2, 1);

        fixture.heap.push(0);
        fixture.assert_index(0, 0);
        fixture.assert_index(1, 2);
        fixture.assert_index(2, 1);
    }
}
