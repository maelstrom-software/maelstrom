use anyhow::Result;
use anyhow_trace::anyhow_trace;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, FromInto};
use std::borrow::BorrowMut;
use std::cmp::{self, Ordering};
use std::marker::PhantomData;
use std::num::NonZeroU64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvlPtr(NonZeroU64);

impl AvlPtr {
    pub fn new(v: u64) -> Option<Self> {
        NonZeroU64::new(v).map(Self)
    }

    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct FlatAvlPtrOption([u8; 8]);

impl From<Option<AvlPtr>> for FlatAvlPtrOption {
    fn from(o: Option<AvlPtr>) -> Self {
        Self(o.map(|v| v.as_u64()).unwrap_or(0).to_be_bytes())
    }
}

impl From<FlatAvlPtrOption> for Option<AvlPtr> {
    fn from(p: FlatAvlPtrOption) -> Self {
        AvlPtr::new(u64::from_be_bytes(p.0))
    }
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AvlNode<KeyT, ValueT> {
    pub key: KeyT,
    pub value: ValueT,
    height: u32,

    #[serde_as(as = "FromInto<FlatAvlPtrOption>")]
    left: Option<AvlPtr>,

    #[serde_as(as = "FromInto<FlatAvlPtrOption>")]
    right: Option<AvlPtr>,
}

impl<KeyT, ValueT> AvlNode<KeyT, ValueT> {
    pub fn new(key: KeyT, value: ValueT) -> Self {
        Self {
            key,
            value,
            height: 1,
            left: None,
            right: None,
        }
    }
}

#[test]
fn avl_node_encoding_size_remains_same() {
    use maelstrom_base::proto;

    let mut n = AvlNode::new(12, 13);
    let start_size = proto::serialized_size(&n).unwrap();
    n.left = Some(AvlPtr::new(77).unwrap());
    n.right = Some(AvlPtr::new(254).unwrap());
    n.height = 100;
    let end_size = proto::serialized_size(&n).unwrap();
    assert_eq!(start_size, end_size);
}

pub trait AvlStorage {
    type Key;
    type Value;

    /// Get the stored root pointer
    async fn root(&mut self) -> Result<Option<AvlPtr>>;

    /// Set the stored root pointer
    async fn set_root(&mut self, root: AvlPtr) -> Result<()>;

    /// Get a node at the given pointer
    async fn look_up(&mut self, ptr: AvlPtr) -> Result<AvlNode<Self::Key, Self::Value>>;

    /// Replace a node at the given pointer. N.B. It is important that a node is never replaced
    /// with a different key or value, this is just for updating the AVL properties
    async fn update(&mut self, ptr: AvlPtr, value: AvlNode<Self::Key, Self::Value>) -> Result<()>;

    /// Add a new node and return its pointer
    async fn insert(&mut self, node: AvlNode<Self::Key, Self::Value>) -> Result<AvlPtr>;

    /// Flush any changes to permanent storage
    async fn flush(&mut self) -> Result<()>;
}

/// An async error capable AVL tree backed something implementing AvlStorage trait. It is intended
/// that the storage trait store entries on disk somehow.
pub struct AvlTree<StorageT> {
    storage: StorageT,
}

#[anyhow_trace]
impl<StorageT: AvlStorage> AvlTree<StorageT>
where
    StorageT::Key: PartialEq + Eq + PartialOrd + Ord,
{
    pub fn new(storage: StorageT) -> Self {
        Self { storage }
    }

    /// Insert a key-value pair in the AVL tree. If there already exists an entry with the given
    /// key, nothing is inserted, and `false` is returned. Otherwise it is inserted and `true` is
    /// returned.
    pub async fn insert_if_not_exists(
        &mut self,
        key: StorageT::Key,
        value: StorageT::Value,
    ) -> Result<bool> {
        if let Some((found, ordering)) = self.binary_search(&key).await? {
            if ordering == Ordering::Equal {
                Ok(false)
            } else {
                self.add_child(found, ordering, key, value).await?;
                Ok(true)
            }
        } else {
            let ptr = self.storage.insert(AvlNode::new(key, value)).await?;
            self.storage.set_root(ptr).await?;
            Ok(true)
        }
    }

    /// Get the value for the given key if it exists in the tree, otherwise `None`
    pub async fn get(&mut self, key: &StorageT::Key) -> Result<Option<StorageT::Value>> {
        let Some((candidate_path, ordering)) = self.binary_search(key).await? else {
            return Ok(None);
        };
        if ordering != Ordering::Equal {
            return Ok(None);
        }
        let candidate = *candidate_path.last().unwrap();
        let node = self.storage.look_up(candidate).await?;
        Ok(Some(node.value))
    }

    /// Update the value for an existing key / value in the tree. The new value **must** serialize
    /// to the same length as the existing value or the tree with be corrupted.
    /// Returns true iff the value was updated.
    pub async fn update_if_exists(
        &mut self,
        key: &StorageT::Key,
        value: StorageT::Value,
    ) -> Result<bool> {
        let Some((candidate_path, ordering)) = self.binary_search(key).await? else {
            return Ok(false);
        };
        if ordering != Ordering::Equal {
            return Ok(false);
        }
        let candidate = *candidate_path.last().unwrap();
        let mut node = self.storage.look_up(candidate).await?;
        node.value = value;
        self.storage.update(candidate, node).await?;
        Ok(true)
    }

    async fn binary_search(
        &mut self,
        key: &StorageT::Key,
    ) -> Result<Option<(Vec<AvlPtr>, Ordering)>> {
        let Some(mut current_ptr) = self.storage.root().await? else {
            return Ok(None);
        };
        let mut path = vec![];
        loop {
            path.push(current_ptr);
            let current = self.storage.look_up(current_ptr).await?;
            let ordering = key.cmp(&current.key);
            match ordering {
                Ordering::Less => {
                    if let Some(left) = current.left {
                        current_ptr = left;
                    } else {
                        return Ok(Some((path, Ordering::Less)));
                    }
                }
                Ordering::Greater => {
                    if let Some(right) = current.right {
                        current_ptr = right;
                    } else {
                        return Ok(Some((path, Ordering::Greater)));
                    }
                }
                Ordering::Equal => {
                    return Ok(Some((path, Ordering::Equal)));
                }
            }
        }
    }

    async fn add_child(
        &mut self,
        parent_path: Vec<AvlPtr>,
        ordering: Ordering,
        key: StorageT::Key,
        value: StorageT::Value,
    ) -> Result<()> {
        let parent = *parent_path.last().unwrap();
        let ptr = self.storage.insert(AvlNode::new(key, value)).await?;
        let mut parent_node = self.storage.look_up(parent).await?;
        match ordering {
            Ordering::Less => {
                assert!(parent_node.left.is_none());
                parent_node.left = Some(ptr);
            }
            Ordering::Greater => {
                assert!(parent_node.right.is_none());
                parent_node.right = Some(ptr);
            }
            Ordering::Equal => {
                unreachable!()
            }
        }
        self.storage.update(parent, parent_node).await?;
        self.do_retracing(parent_path).await?;
        Ok(())
    }

    /// A sorted iterator over all the entries in the tree
    #[cfg(test)]
    pub async fn entries(
        &mut self,
    ) -> Result<impl futures::Stream<Item = Result<(StorageT::Key, StorageT::Value)>> + '_> {
        let walker = AvlTreeWalker::new(self).await?;
        Ok(futures::stream::unfold(walker, |mut walker| async move {
            walker.next().await.transpose().map(|v| (v, walker))
        }))
    }

    pub async fn into_stream(
        self,
    ) -> Result<impl futures::Stream<Item = Result<(StorageT::Key, StorageT::Value)>>> {
        let walker = AvlTreeWalker::new(self).await?;
        Ok(futures::stream::unfold(walker, |mut walker| async move {
            walker.next().await.transpose().map(|v| (v, walker))
        }))
    }

    async fn update_height(
        &mut self,
        node: &mut AvlNode<StorageT::Key, StorageT::Value>,
    ) -> Result<()> {
        let right_height = self.get_height(node.right).await?;
        let left_height = self.get_height(node.left).await?;
        node.height = 1 + cmp::max(left_height, right_height);
        Ok(())
    }

    async fn retrace_one(&mut self, c_ptr: AvlPtr) -> Result<Option<(AvlPtr, AvlPtr)>> {
        let mut c = self.storage.look_up(c_ptr).await?;
        let balance_factor = self.balance_factor(&c).await?;
        if !(-1..=1).contains(&balance_factor) {
            Ok(Some((c_ptr, self.rebalance(c_ptr, balance_factor).await?)))
        } else {
            self.update_height(&mut c).await?;
            self.storage.update(c_ptr, c).await?;
            Ok(None)
        }
    }

    async fn replace_child(
        &mut self,
        parent_ptr: AvlPtr,
        old_child: AvlPtr,
        new_child: AvlPtr,
    ) -> Result<()> {
        let mut parent = self.storage.look_up(parent_ptr).await?;
        if parent.left == Some(old_child) {
            parent.left = Some(new_child);
        } else {
            parent.right = Some(new_child);
        }
        self.storage.update(parent_ptr, parent).await?;
        Ok(())
    }

    async fn do_retracing(&mut self, mut path: Vec<AvlPtr>) -> Result<()> {
        let mut new_child = None;
        while let Some(c_ptr) = path.pop() {
            if let Some((old, new)) = new_child.take() {
                self.replace_child(c_ptr, old, new).await?;
            }
            new_child = self.retrace_one(c_ptr).await?;
        }
        if let Some((_, new)) = new_child {
            self.storage.set_root(new).await?;
        }
        Ok(())
    }

    async fn balance_factor_ptr(&mut self, node: AvlPtr) -> Result<i32> {
        let node = self.storage.look_up(node).await?;
        self.balance_factor(&node).await
    }

    async fn balance_factor(
        &mut self,
        node: &AvlNode<StorageT::Key, StorageT::Value>,
    ) -> Result<i32> {
        let right_height = self.get_height(node.right).await?;
        let left_height = self.get_height(node.left).await?;
        Ok(right_height as i32 - left_height as i32)
    }

    async fn rebalance(&mut self, node_ptr: AvlPtr, balance_factor: i32) -> Result<AvlPtr> {
        let node = self.storage.look_up(node_ptr).await?;
        if balance_factor > 0 {
            if self.balance_factor_ptr(node.right.unwrap()).await? < 0 {
                Ok(self.do_right_left_rotation(node_ptr).await?)
            } else {
                Ok(self.do_left_rotation(node_ptr).await?)
            }
        } else if self.balance_factor_ptr(node.left.unwrap()).await? > 0 {
            Ok(self.do_left_right_rotation(node_ptr).await?)
        } else {
            Ok(self.do_right_rotation(node_ptr).await?)
        }
    }

    async fn get_height(&mut self, node: Option<AvlPtr>) -> Result<u32> {
        if let Some(node) = node {
            Ok(self.storage.look_up(node).await?.height)
        } else {
            Ok(0)
        }
    }

    async fn do_left_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.look_up(old_root_ptr).await?;
        let new_root_ptr = old_root.right.take().unwrap();
        let mut new_root = self.storage.look_up(new_root_ptr).await?;
        let old_left = new_root.left.replace(old_root_ptr);
        old_root.right = old_left;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    async fn do_right_left_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.look_up(old_root_ptr).await?;
        let mut new_root_ptr = old_root.right.take().unwrap();
        new_root_ptr = self.do_right_rotation(new_root_ptr).await?;
        let mut new_root = self.storage.look_up(new_root_ptr).await?;
        let old_left = new_root.left.replace(old_root_ptr);
        old_root.right = old_left;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    async fn do_right_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.look_up(old_root_ptr).await?;
        let new_root_ptr = old_root.left.take().unwrap();
        let mut new_root = self.storage.look_up(new_root_ptr).await?;
        let old_right = new_root.right.replace(old_root_ptr);
        old_root.left = old_right;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    async fn do_left_right_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.look_up(old_root_ptr).await?;
        let mut new_root_ptr = old_root.left.take().unwrap();
        new_root_ptr = self.do_left_rotation(new_root_ptr).await?;
        let mut new_root = self.storage.look_up(new_root_ptr).await?;
        let old_right = new_root.right.replace(old_root_ptr);
        old_root.left = old_right;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.storage.flush().await
    }

    #[cfg(test)]
    async fn height_of(&mut self, node_ptr: Option<AvlPtr>) -> Result<usize> {
        let Some(node_ptr) = node_ptr else {
            return Ok(0);
        };
        let node = self.storage.look_up(node_ptr).await?;
        let mut layer: Vec<_> = [node.left, node.right].into_iter().flatten().collect();
        let mut next_layer = vec![];
        let mut height = 1;
        while !layer.is_empty() {
            for entry in layer {
                let node = self.storage.look_up(entry).await?;
                next_layer.extend([node.left, node.right].into_iter().flatten());
            }
            layer = next_layer;
            next_layer = vec![];
            height += 1;
        }
        Ok(height)
    }

    #[cfg(test)]
    async fn assert_invariants(&mut self) -> Result<()> {
        let mut next: Vec<_> = self.storage.root().await?.into_iter().collect();
        while let Some(current) = next.pop() {
            let node = self.storage.look_up(current).await?;
            let left_height = self.height_of(node.left).await? as isize;
            let right_height = self.height_of(node.right).await? as isize;
            let balance_factor = left_height - right_height;
            assert_eq!(
                node.height,
                (cmp::max(left_height, right_height) + 1) as u32
            );
            assert!(
                (-1..=1).contains(&balance_factor),
                "balance factor wrong: -1 <= {balance_factor} <= 1"
            );
            next.extend([node.left, node.right].into_iter().flatten());
        }
        Ok(())
    }
}

#[cfg(test)]
impl<StorageT: AvlStorage> AvlTree<StorageT>
where
    StorageT::Key: PartialEq + Eq + PartialOrd + Ord + std::fmt::Debug,
{
    async fn print(&mut self) -> Result<()> {
        println!("tree:");
        let Some(node_ptr) = self.storage.root().await? else {
            println!("<empty>");
            return Ok(());
        };
        let node = self.storage.look_up(node_ptr).await?;
        println!("{:?}(h={}) ", node.key, node.height);
        let mut layer = vec![node.left, node.right];
        let mut next_layer = vec![];
        while !layer.is_empty() {
            for entry in layer {
                if let Some(entry) = entry {
                    let node = self.storage.look_up(entry).await?;
                    print!("{:?}(h={}) ", node.key, node.height);
                    next_layer.extend([node.left, node.right]);
                } else {
                    print!("X ");
                }
            }
            println!();
            layer = next_layer;
            next_layer = vec![];
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Visited {
    None,
    Left,
    Own,
    Right,
}

struct AvlTreeWalker<TreeT, StorageT> {
    tree: TreeT,
    stack: Vec<(AvlPtr, Visited)>,
    storage: PhantomData<StorageT>,
}

impl<TreeT: BorrowMut<AvlTree<StorageT>>, StorageT: AvlStorage> AvlTreeWalker<TreeT, StorageT> {
    async fn new(mut tree: TreeT) -> Result<Self> {
        let mut stack = vec![];
        if let Some(current) = tree.borrow_mut().storage.root().await? {
            stack.push((current, Visited::None));
        }
        Ok(Self {
            tree,
            stack,
            storage: PhantomData,
        })
    }

    async fn advance(&mut self) -> Result<()> {
        let (ptr, state) = self.stack.pop().unwrap();
        let node = self.tree.borrow_mut().storage.look_up(ptr).await?;
        match state {
            Visited::None => {
                self.stack.push((ptr, Visited::Left));
                if let Some(left) = node.left {
                    self.stack.push((left, Visited::None));
                }
            }
            Visited::Own => {
                self.stack.push((ptr, Visited::Right));
                if let Some(right) = node.right {
                    self.stack.push((right, Visited::None));
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<(StorageT::Key, StorageT::Value)>> {
        loop {
            if let Some((_, state)) = self.stack.last() {
                if state == &Visited::Left {
                    break;
                }
            } else {
                return Ok(None);
            }

            self.advance().await?;
        }

        let (top, _) = self.stack.pop().unwrap();
        self.stack.push((top, Visited::Own));
        let top = self.tree.borrow_mut().storage.look_up(top).await?;
        Ok(Some((top.key, top.value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use maelstrom_util::ext::BoolExt as _;

    #[derive(Default)]
    struct MemoryStorage {
        root: Option<AvlPtr>,
        values: Vec<AvlNode<u32, u32>>,
    }

    impl AvlStorage for MemoryStorage {
        type Key = u32;
        type Value = u32;

        async fn root(&mut self) -> Result<Option<AvlPtr>> {
            Ok(self.root)
        }

        async fn set_root(&mut self, root: AvlPtr) -> Result<()> {
            self.root = Some(root);
            Ok(())
        }

        async fn look_up(&mut self, key: AvlPtr) -> Result<AvlNode<u32, u32>> {
            Ok(self.values[key.as_u64() as usize - 1].clone())
        }

        async fn update(&mut self, key: AvlPtr, value: AvlNode<u32, u32>) -> Result<()> {
            self.values[(key.as_u64() as usize) - 1] = value;
            Ok(())
        }

        async fn insert(&mut self, node: AvlNode<u32, u32>) -> Result<AvlPtr> {
            self.values.push(node);
            Ok(AvlPtr::new(self.values.len() as u64).unwrap())
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    async fn test_it(mut expected: Vec<u32>) {
        let mut tree = AvlTree::new(MemoryStorage::default());
        for v in &expected {
            tree.insert_if_not_exists(*v, *v)
                .await
                .unwrap()
                .assert_is_true();
            tree.print().await.unwrap();
            tree.assert_invariants().await.unwrap();
        }
        expected.sort();
        let expected: Vec<_> = expected.into_iter().map(|v| (v, v)).collect();
        let tree_iter = tree.entries().await.unwrap();
        let actual: Vec<_> = tree_iter.map(|v| v.unwrap()).collect().await;
        assert_eq!(actual, expected);

        for (k, v) in &expected {
            let value = tree.get(k).await.unwrap().unwrap();
            assert_eq!(value, *v);
        }
    }

    #[tokio::test]
    async fn insert_iter_get() {
        test_it(vec![11, 7]).await;
        test_it(vec![11, 7, 5, 9, 2, 1]).await;
        test_it(vec![7, 2, 5, 88, 1]).await;
        test_it(vec![100, 7, 200, 1, 222, 3]).await;
        test_it(vec![
            67, 77, 23, 43, 65, 3, 61, 60, 34, 56, 68, 85, 2, 99, 45, 93, 74, 54, 92, 90,
        ])
        .await;
        test_it(vec![
            4, 50, 83, 29, 13, 88, 51, 89, 6, 99, 30, 19, 44, 18, 96, 43, 59, 57, 65, 78,
        ])
        .await;
        test_it(vec![
            67, 71, 96, 8, 89, 62, 12, 76, 55, 13, 74, 36, 85, 24, 69, 27, 37, 81, 19, 73,
        ])
        .await;
        test_it(vec![
            99, 1, 59, 87, 86, 34, 36, 50, 78, 66, 28, 81, 56, 69, 71, 7, 62, 16, 74, 48,
        ])
        .await;
        test_it(vec![
            13, 21, 90, 81, 54, 32, 52, 25, 29, 5, 19, 75, 33, 68, 49, 41, 55, 42, 37, 6,
        ])
        .await;
        test_it(vec![
            68, 2, 59, 4, 0, 56, 47, 70, 58, 19, 8, 61, 28, 35, 1, 13, 63, 29, 46, 69,
        ])
        .await;
        test_it(vec![
            70, 62, 17, 75, 93, 4, 6, 56, 11, 30, 32, 47, 9, 96, 33, 18, 58, 82, 85, 60,
        ])
        .await;
        test_it(vec![
            20, 13, 60, 57, 21, 73, 63, 0, 9, 5, 82, 48, 39, 2, 23, 88, 40, 83, 42, 94,
        ])
        .await;
        test_it(vec![
            53, 26, 83, 51, 27, 9, 23, 99, 39, 34, 3, 10, 32, 77, 40, 72, 93, 12, 66, 96,
        ])
        .await;
        test_it(vec![
            23, 83, 80, 49, 5, 96, 15, 28, 74, 90, 64, 0, 67, 76, 9, 87, 55, 75, 99, 60,
        ])
        .await;
        test_it(vec![]).await;
        test_it((0..100).collect()).await;
        test_it((0..100).rev().collect()).await;
    }

    #[tokio::test]
    async fn get_not_found() {
        let mut tree = AvlTree::new(MemoryStorage::default());
        tree.insert_if_not_exists(1, 4)
            .await
            .unwrap()
            .assert_is_true();
        tree.insert_if_not_exists(2, 5)
            .await
            .unwrap()
            .assert_is_true();
        tree.insert_if_not_exists(3, 6)
            .await
            .unwrap()
            .assert_is_true();
        assert_eq!(tree.get(&7).await.unwrap(), None);
    }

    #[tokio::test]
    async fn update_if_exists() {
        let mut tree = AvlTree::new(MemoryStorage::default());
        tree.insert_if_not_exists(1, 4)
            .await
            .unwrap()
            .assert_is_true();
        tree.update_if_exists(&2, 5)
            .await
            .unwrap()
            .assert_is_false();
        tree.update_if_exists(&1, 7).await.unwrap().assert_is_true();
        assert_eq!(tree.get(&1).await.unwrap(), Some(7));
        assert_eq!(tree.get(&2).await.unwrap(), None);
    }
}
