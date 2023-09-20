//! A binary min-heap implementation that provides some features necessary for us that are missing
//! from [std::collections::BinaryHeap].

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// A min-heap implementation that provides some features necessary for us that are missing from
/// [std::collections::BinaryHeap]:
///
///   - The values (priorities) of elements can be stored outside of the heap itself. This supports
///     a common use case where the heap just contains IDs for a specific data structure, and the
///     actual data structures are stored elsewhere, usually in a map of some sort.
///   - The heap can be notified when Individual elements' values change, via [Heap::sift_up] and
///     [Heap::sift_down], both of which are O(log(n)).
///   - If all or most of the elements' values change, the heap can be fixed up using
///     [Heap::rebuild]. This is O(n).
///   - Arbitrary elements can be removed from the heap in O(log(n)) time.
///
/// This heap accomplishes this by telling the elements what their heap indices are. Those heap
/// indices are then used in the per-element methods.
///
/// Since the values of elements are stored externally from the heap, the heap relies on the
/// client notifying it whenever a value changes. If the client doesn't do this, the heap condition
/// will be violated and [Heap::peek] will no longer necessarily provide the smallest element.
///
/// The modifying methods of this struct take a [&mut HeapDeps], instead of a [Heap] constructor
/// taking the dependencies and storing them in the heap. This is done to satisfy the borrow
/// checker so that clients of the heap don't need to use a bunch of Rc<Refcell<_>>s.
pub struct Heap<DepsT: HeapDeps>(Vec<DepsT::Element>);

/// An index value in [Heap]. These are provided to the client via [HeapDeps::update_index]. The
/// client then provides them back to the heap when calling [Heap::sift_up], [Heap::sift_down], and
/// [Heap::remove].
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct HeapIndex(usize);

/// A client of the [Heap] must implement this trait.
pub trait HeapDeps {
    /// The type of elements to be stored in the heap.
    type Element;

    /// Compare two elements. This is defined on [HeapDeps], instead of just using [Ord] on the
    /// element type, because the client will usually want to consult external data to determine
    /// the ordering of two elements.
    fn is_element_less_than(&self, lhs: &Self::Element, rhs: &Self::Element) -> bool;

    /// Update the stored index of a given element. This index can be used for [Heap::remove],
    /// [Heap::sift_up], and [Heap::sift_down]. The methods of [Heap] will call this at most once
    /// for a given element in a given method call.
    fn update_index(&mut self, elem: &Self::Element, idx: HeapIndex);
}

impl<DepsT: HeapDeps> Default for Heap<DepsT> {
    fn default() -> Self {
        Heap(Vec::default())
    }
}

impl<DepsT: HeapDeps> Heap<DepsT> {
    /// Return an [Option] containing an element with the smallest value in the heap, or [None] if
    /// the heap is empty. Note that multiple elements in the heap may have the smallest value. In
    /// this case, an arbitrary element will be returned. O(1).
    pub fn peek(&self) -> Option<&DepsT::Element> {
        self.0.get(0)
    }

    /// Remove the element with the smallest value in the heap, or [None] if the heap is empty.
    /// Note that multiple elements in the heap may have the smallest value. In this case, an
    /// arbitrary element will be returned. O(log(n)).
    #[allow(dead_code)]
    pub fn pop(&mut self, deps: &mut DepsT) -> Option<DepsT::Element> {
        match self.0.len() {
            0 => None,
            1 => Some(self.0.remove(0)),
            _ => {
                let elem = self.0.swap_remove(0);
                let idx = self.sift_down_internal(deps, HeapIndex(0), true);
                self.update_index(deps, idx);
                Some(elem)
            }
        }
    }

    /// Add a new element to the heap. O(log(n)).
    pub fn push(&mut self, deps: &mut DepsT, elem: DepsT::Element) {
        let idx = HeapIndex(self.0.len());
        self.0.push(elem);
        let idx = self.sift_up_internal(deps, idx);
        self.update_index(deps, idx);
    }

    /// Remove the element at the given index from the heap. If the index is out of range, the
    /// function will panic. O(log(n)).
    pub fn remove(&mut self, deps: &mut DepsT, idx: HeapIndex) {
        self.0.swap_remove(idx.0);
        if idx.0 < self.0.len() {
            let mut new_idx = self.sift_up_internal(deps, idx);
            if new_idx == idx {
                new_idx = self.sift_down_internal(deps, idx, true);
            }
            self.update_index(deps, new_idx);
        }
    }

    /// Notify the heap an element's value has decreased. The heap will see if the element needs to
    /// be sifted up the heap. If the index is out of range, the function will panic. O(log(n)).
    pub fn sift_up(&mut self, deps: &mut DepsT, idx: HeapIndex) {
        let new_idx = self.sift_up_internal(deps, idx);
        if new_idx != idx {
            self.update_index(deps, new_idx);
        }
    }

    /// Notify the heap an element's value has increased. The heap will see if the element needs to
    /// be sifted down the heap. If the index is out of range, the function will panic. O(log(n)).
    pub fn sift_down(&mut self, deps: &mut DepsT, idx: HeapIndex) {
        let new_idx = self.sift_down_internal(deps, idx, true);
        if new_idx != idx {
            self.update_index(deps, new_idx);
        }
    }

    /// Assume all elements' values have changed and rebuild the heap accordingly. If one is
    /// changing the values of most of the elements in the heap, this method is faster than calling
    /// [Heap::sift_up] or [Heap::sift_down] on each element. O(n).
    pub fn rebuild(&mut self, deps: &mut DepsT) {
        // The last ceil(n/2) elements form single-element sub heaps. We walk backwards from there
        // sifting down the remaining floor(n/2) elements.
        for idx in (0..self.0.len() / 2).rev() {
            self.sift_down_internal(deps, HeapIndex(idx), false);
        }
        for (idx, elem) in self.0.iter().enumerate() {
            deps.update_index(elem, HeapIndex(idx));
        }
    }
}

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

impl<DepsT: HeapDeps> Heap<DepsT> {
    fn update_index(&mut self, deps: &mut DepsT, idx: HeapIndex) {
        deps.update_index(&self.0[idx.0], idx);
    }

    fn sift_up_internal(&mut self, deps: &mut DepsT, HeapIndex(mut idx): HeapIndex) -> HeapIndex {
        while idx != 0 {
            let parent_idx = (idx + 1) / 2 - 1;

            if !deps.is_element_less_than(&self.0[idx], &self.0[parent_idx]) {
                break;
            }

            self.0.swap(parent_idx, idx);
            self.update_index(deps, HeapIndex(idx));
            idx = parent_idx;
        }
        HeapIndex(idx)
    }

    fn sift_down_internal(
        &mut self,
        deps: &mut DepsT,
        HeapIndex(mut idx): HeapIndex,
        update_index: bool,
    ) -> HeapIndex {
        loop {
            let mut child_idx = (idx + 1) * 2 - 1;
            if child_idx >= self.0.len() {
                break;
            }
            if child_idx + 1 < self.0.len()
                && !deps.is_element_less_than(&self.0[child_idx], &self.0[child_idx + 1])
            {
                child_idx += 1;
            }

            if !deps.is_element_less_than(&self.0[child_idx], &self.0[idx]) {
                break;
            }

            self.0.swap(child_idx, idx);
            if update_index {
                self.update_index(deps, HeapIndex(idx));
            }
            idx = child_idx;
        }
        HeapIndex(idx)
    }
}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct TestElement {
        weight: i32,
        heap_index: HeapIndex,
    }

    impl HeapDeps for HashMap<u64, TestElement> {
        type Element = u64;

        fn is_element_less_than(&self, lhs: &u64, rhs: &u64) -> bool {
            self.get(lhs).unwrap().weight < self.get(rhs).unwrap().weight
        }

        fn update_index(&mut self, id: &u64, idx: HeapIndex) {
            self.get_mut(id).unwrap().heap_index = idx;
        }
    }

    #[derive(Default)]
    struct Fixture {
        elements: HashMap<u64, TestElement>,
        heap: Heap<HashMap<u64, TestElement>>,
    }

    impl Fixture {
        fn validate_indices(&self) {
            for (idx, id) in self.heap.0.iter().enumerate() {
                assert_eq!(self.elements.get(&id).unwrap().heap_index, HeapIndex(idx));
            }
        }

        fn validate_heap_property(&self) {
            for idx in 1..self.heap.0.len() {
                let parent_idx = (idx + 1) / 2 - 1;
                let parent_id = self.heap.0[parent_idx];
                let id = self.heap.0[idx];
                assert!(
                    self.elements.get(&parent_id).unwrap().weight
                        < self.elements.get(&id).unwrap().weight
                );
            }
        }

        fn validate(&self) {
            self.validate_heap_property();
            self.validate_indices();
        }

        fn push(&mut self, id: u64, weight: i32) {
            assert!(self
                .elements
                .insert(
                    id,
                    TestElement {
                        weight,
                        heap_index: HeapIndex::default()
                    }
                )
                .is_none());
            self.heap.push(&mut self.elements, id);
        }

        fn remove(&mut self, id: u64) {
            let elem = self.elements.remove(&id).unwrap();
            self.heap.remove(&mut self.elements, elem.heap_index);
        }

        fn pop(&mut self) -> Option<(u64, i32)> {
            self.heap
                .pop(&mut self.elements)
                .map(|id| (id, self.elements.remove(&id).unwrap().weight))
        }

        fn reweigh(&mut self, id: u64, new_weight: i32) {
            let elem = self.elements.get_mut(&id).unwrap();
            let old_weight = elem.weight;
            elem.weight = new_weight;
            let heap_index = elem.heap_index;
            if new_weight < old_weight {
                self.heap.sift_up(&mut self.elements, heap_index)
            } else {
                self.heap.sift_down(&mut self.elements, heap_index)
            }
        }
    }

    #[test]
    fn peek_on_empty_and_small_heaps() {
        let mut fixture = Fixture::default();

        assert_eq!(fixture.heap.peek().copied(), None);
        assert_eq!(fixture.heap.peek().copied(), None);

        fixture.push(1, 1000);
        assert_eq!(fixture.heap.peek().copied(), Some(1));
        assert_eq!(fixture.heap.peek().copied(), Some(1));

        fixture.push(2, 0);
        assert_eq!(fixture.heap.peek().copied(), Some(2));
    }

    #[test]
    fn push_ascending() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, 1000 * i as i32);
            fixture.validate();
        }
    }

    #[test]
    fn push_descending() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, -1000 * i as i32);
            fixture.validate();
        }
    }

    #[test]
    fn push_random() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, rand::random());
            fixture.validate();
        }
    }

    #[test]
    fn pop_all_ascending() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, 1000 * i as i32);
        }
        for i in 0..1000 {
            let (id, weight) = fixture.pop().unwrap();
            assert_eq!(i, id);
            assert_eq!(weight, 1000 * i as i32);
        }
        assert_eq!(fixture.pop(), None);
        assert_eq!(fixture.pop(), None);
        assert_eq!(fixture.pop(), None);
    }

    #[test]
    fn pop_all_descending() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, -1000 * i as i32);
        }
        for i in 0..1000 {
            let (id, weight) = fixture.pop().unwrap();
            assert_eq!(i, 999 - id);
            assert_eq!(weight, -1000 * id as i32);
        }
        assert_eq!(fixture.pop(), None);
        assert_eq!(fixture.pop(), None);
        assert_eq!(fixture.pop(), None);
    }

    #[test]
    fn remove_random() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, rand::random());
            fixture.validate();
        }
        for i in 0..1000 {
            fixture.remove(i);
            fixture.validate();
        }
    }

    #[test]
    fn sift_up_and_sift_down_random() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, rand::random());
            fixture.validate();
        }
        for _ in 0..10 {
            for i in 0..1000 {
                fixture.reweigh(i, rand::random());
                fixture.validate();
            }
        }
    }

    #[test]
    fn rebuild_random() {
        let mut fixture = Fixture::default();
        for i in 0..1000 {
            fixture.push(i, rand::random());
            fixture.validate();
        }
        for _ in 0..10 {
            for elem in fixture.elements.values_mut() {
                elem.weight = rand::random();
            }
            fixture.heap.rebuild(&mut fixture.elements);
            fixture.validate();
        }
    }
}
