use anyhow::Result;
use async_trait::async_trait;
use std::cmp::{self, Ordering};
use std::mem;
use std::num::NonZeroU64;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct AvlPtr(NonZeroU64);

impl AvlPtr {
    pub fn new(v: u64) -> Option<Self> {
        NonZeroU64::new(v).map(Self)
    }

    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug)]
pub struct AvlNode<KeyT, ValueT> {
    pub key: KeyT,
    pub value: ValueT,
    height: u32,
    left: Option<AvlPtr>,
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

#[async_trait]
pub trait AvlStorage {
    type Key;
    type Value;
    async fn root(&self) -> Result<Option<AvlPtr>>;
    async fn set_root(&mut self, root: AvlPtr) -> Result<()>;
    async fn lookup(&self, ptr: AvlPtr) -> Result<AvlNode<Self::Key, Self::Value>>;
    async fn update(&mut self, ptr: AvlPtr, value: AvlNode<Self::Key, Self::Value>) -> Result<()>;
    async fn insert(&mut self, node: AvlNode<Self::Key, Self::Value>) -> Result<AvlPtr>;
}

pub struct AvlTree<StorageT> {
    storage: StorageT,
}

impl<StorageT: AvlStorage> AvlTree<StorageT>
where
    StorageT::Key: PartialEq + Eq + PartialOrd + Ord,
{
    pub fn new(storage: StorageT) -> Self {
        Self { storage }
    }

    pub async fn insert(
        &mut self,
        key: StorageT::Key,
        value: StorageT::Value,
    ) -> Result<Option<StorageT::Value>> {
        if let Some((found, ordering)) = self.binary_search(&key).await? {
            if ordering == Ordering::Equal {
                let found = *found.last().unwrap();
                let mut existing = self.storage.lookup(found).await?;
                let old_value = mem::replace(&mut existing.value, value);
                self.storage.update(found, existing).await?;
                Ok(Some(old_value))
            } else {
                self.add_child(found, ordering, key, value).await?;
                Ok(None)
            }
        } else {
            let ptr = self.storage.insert(AvlNode::new(key, value)).await?;
            self.storage.set_root(ptr).await?;
            Ok(None)
        }
    }

    pub async fn get(&self, key: &StorageT::Key) -> Result<Option<StorageT::Value>> {
        let Some((candidate_path, ordering)) = self.binary_search(key).await? else {
            return Ok(None);
        };
        if ordering != Ordering::Equal {
            return Ok(None);
        }
        let candidate = *candidate_path.last().unwrap();
        let node = self.storage.lookup(candidate).await?;
        Ok(Some(node.value))
    }

    async fn binary_search(&self, key: &StorageT::Key) -> Result<Option<(Vec<AvlPtr>, Ordering)>> {
        let Some(mut current_ptr) = self.storage.root().await? else {
            return Ok(None);
        };
        let mut path = vec![];
        loop {
            path.push(current_ptr);
            let current = self.storage.lookup(current_ptr).await?;
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
        let mut parent_node = self.storage.lookup(parent).await?;
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

    pub async fn entries(
        &self,
    ) -> Result<impl futures::Stream<Item = Result<(StorageT::Key, StorageT::Value)>> + '_> {
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
        let mut c = self.storage.lookup(c_ptr).await?;
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
        let mut parent = self.storage.lookup(parent_ptr).await?;
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

    async fn balance_factor_ptr(&self, node: AvlPtr) -> Result<i32> {
        let node = self.storage.lookup(node).await?;
        self.balance_factor(&node).await
    }

    async fn balance_factor(&self, node: &AvlNode<StorageT::Key, StorageT::Value>) -> Result<i32> {
        let right_height = self.get_height(node.right).await?;
        let left_height = self.get_height(node.left).await?;
        Ok(right_height as i32 - left_height as i32)
    }

    async fn rebalance(&mut self, node_ptr: AvlPtr, balance_factor: i32) -> Result<AvlPtr> {
        let node = self.storage.lookup(node_ptr).await?;
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

    async fn get_height(&self, node: Option<AvlPtr>) -> Result<u32> {
        if let Some(node) = node {
            Ok(self.storage.lookup(node).await?.height)
        } else {
            Ok(0)
        }
    }

    async fn do_left_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.lookup(old_root_ptr).await?;
        let new_root_ptr = old_root.right.take().unwrap();
        let mut new_root = self.storage.lookup(new_root_ptr).await?;
        let old_left = new_root.left.replace(old_root_ptr);
        old_root.right = old_left;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    async fn do_right_left_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.lookup(old_root_ptr).await?;
        let mut new_root_ptr = old_root.right.take().unwrap();
        new_root_ptr = self.do_right_rotation(new_root_ptr).await?;
        let mut new_root = self.storage.lookup(new_root_ptr).await?;
        let old_left = new_root.left.replace(old_root_ptr);
        old_root.right = old_left;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    async fn do_right_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.lookup(old_root_ptr).await?;
        let new_root_ptr = old_root.left.take().unwrap();
        let mut new_root = self.storage.lookup(new_root_ptr).await?;
        let old_right = new_root.right.replace(old_root_ptr);
        old_root.left = old_right;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    async fn do_left_right_rotation(&mut self, old_root_ptr: AvlPtr) -> Result<AvlPtr> {
        let mut old_root = self.storage.lookup(old_root_ptr).await?;
        let mut new_root_ptr = old_root.left.take().unwrap();
        new_root_ptr = self.do_left_rotation(new_root_ptr).await?;
        let mut new_root = self.storage.lookup(new_root_ptr).await?;
        let old_right = new_root.right.replace(old_root_ptr);
        old_root.left = old_right;

        self.update_height(&mut old_root).await?;
        self.storage.update(old_root_ptr, old_root).await?;
        self.update_height(&mut new_root).await?;
        self.storage.update(new_root_ptr, new_root).await?;
        Ok(new_root_ptr)
    }

    #[cfg(test)]
    async fn height_of(&self, node_ptr: Option<AvlPtr>) -> Result<usize> {
        let Some(node_ptr) = node_ptr else {
            return Ok(0);
        };
        let node = self.storage.lookup(node_ptr).await?;
        let mut layer: Vec<_> = [node.left, node.right].into_iter().flatten().collect();
        let mut next_layer = vec![];
        let mut height = 1;
        while !layer.is_empty() {
            for entry in layer {
                let node = self.storage.lookup(entry).await?;
                next_layer.extend([node.left, node.right].into_iter().flatten());
            }
            layer = next_layer;
            next_layer = vec![];
            height += 1;
        }
        Ok(height)
    }

    #[cfg(test)]
    async fn assert_invariants(&self) -> Result<()> {
        let mut next: Vec<_> = self.storage.root().await?.into_iter().collect();
        while let Some(current) = next.pop() {
            let node = self.storage.lookup(current).await?;
            let left_height = self.height_of(node.left).await? as isize;
            let right_height = self.height_of(node.right).await? as isize;
            let balance_factor = left_height - right_height;
            assert_eq!(
                node.height,
                (cmp::max(left_height, right_height) + 1) as u32
            );
            assert!(
                balance_factor >= -1 && balance_factor <= 1,
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
    async fn print(&self) -> Result<()> {
        println!("tree:");
        let Some(node_ptr) = self.storage.root().await? else {
            println!("<empty>");
            return Ok(());
        };
        let node = self.storage.lookup(node_ptr).await?;
        println!("{:?}(h={}) ", node.key, node.height);
        let mut layer = vec![node.left, node.right];
        let mut next_layer = vec![];
        while !layer.is_empty() {
            for entry in layer {
                if let Some(entry) = entry {
                    let node = self.storage.lookup(entry).await?;
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

struct AvlTreeWalker<'tree, StorageT> {
    tree: &'tree AvlTree<StorageT>,
    stack: Vec<(AvlPtr, Visited)>,
}

impl<'tree, StorageT: AvlStorage> AvlTreeWalker<'tree, StorageT> {
    async fn new(tree: &'tree AvlTree<StorageT>) -> Result<Self> {
        let mut stack = vec![];
        if let Some(current) = tree.storage.root().await? {
            stack.push((current, Visited::None));
        }
        Ok(Self { tree, stack })
    }

    async fn advance(&mut self) -> Result<()> {
        let (ptr, state) = self.stack.pop().unwrap();
        let node = self.tree.storage.lookup(ptr).await?;
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
        let top = self.tree.storage.lookup(top).await?;
        Ok(Some((top.key, top.value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use maelstrom_util::ext::OptionExt as _;

    #[derive(Default)]
    struct MemoryStorage {
        root: Option<AvlPtr>,
        values: Vec<AvlNode<u32, u32>>,
    }

    #[async_trait]
    impl AvlStorage for MemoryStorage {
        type Key = u32;
        type Value = u32;

        async fn root(&self) -> Result<Option<AvlPtr>> {
            Ok(self.root)
        }

        async fn set_root(&mut self, root: AvlPtr) -> Result<()> {
            self.root = Some(root);
            Ok(())
        }

        async fn lookup(&self, key: AvlPtr) -> Result<AvlNode<u32, u32>> {
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
    }

    async fn insert_iter_get_test(mut expected: Vec<u32>) {
        let mut tree = AvlTree::new(MemoryStorage::default());
        for v in &expected {
            tree.insert(*v, *v).await.unwrap().assert_is_none();
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
        insert_iter_get_test(vec![11, 7]).await;
        insert_iter_get_test(vec![11, 7, 5, 9, 2, 1]).await;
        insert_iter_get_test(vec![7, 2, 5, 88, 1]).await;
        insert_iter_get_test(vec![100, 7, 200, 1, 222, 3]).await;
        insert_iter_get_test(vec![]).await;
        insert_iter_get_test((0..100).collect()).await;
        insert_iter_get_test((0..100).rev().collect()).await;
    }

    #[tokio::test]
    async fn get_not_found() {
        let mut tree = AvlTree::new(MemoryStorage::default());
        tree.insert(1, 4).await.unwrap().assert_is_none();
        tree.insert(2, 5).await.unwrap().assert_is_none();
        tree.insert(3, 6).await.unwrap().assert_is_none();
        assert_eq!(tree.get(&7).await.unwrap(), None);
    }
}
