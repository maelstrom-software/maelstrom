use anyhow::Result;
use async_trait::async_trait;
use std::cmp::Ordering;
use std::mem;
use std::num::NonZeroU64;

#[derive(Copy, Clone, Debug)]
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
    // height: u32,
    left: Option<AvlPtr>,
    right: Option<AvlPtr>,
}

impl<KeyT, ValueT> AvlNode<KeyT, ValueT> {
    pub fn new(key: KeyT, value: ValueT) -> Self {
        Self {
            key,
            value,
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
        let Some((candidate, ordering)) = self.binary_search(key).await? else {
            return Ok(None);
        };
        if ordering != Ordering::Equal {
            return Ok(None);
        }
        let node = self.storage.lookup(candidate).await?;
        Ok(Some(node.value))
    }

    async fn binary_search(&self, key: &StorageT::Key) -> Result<Option<(AvlPtr, Ordering)>> {
        let Some(mut current_ptr) = self.storage.root().await? else {
            return Ok(None);
        };
        loop {
            let current = self.storage.lookup(current_ptr).await?;
            let ordering = key.cmp(&current.key);
            match ordering {
                Ordering::Less => {
                    if let Some(left) = current.left {
                        current_ptr = left;
                    } else {
                        return Ok(Some((current_ptr, Ordering::Less)));
                    }
                }
                Ordering::Greater => {
                    if let Some(right) = current.right {
                        current_ptr = right;
                    } else {
                        return Ok(Some((current_ptr, Ordering::Greater)));
                    }
                }
                Ordering::Equal => {
                    return Ok(Some((current_ptr, Ordering::Equal)));
                }
            }
        }
    }

    async fn add_child(
        &mut self,
        parent: AvlPtr,
        ordering: Ordering,
        key: StorageT::Key,
        value: StorageT::Value,
    ) -> Result<()> {
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
        insert_iter_get_test((0..1000).collect()).await;
        insert_iter_get_test((0..1000).rev().collect()).await;
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
