use std::{future::Future, num::NonZeroU32, sync::Mutex};
use tokio::sync::Semaphore;

pub async fn await_and_every_sec<RetT>(
    mut fut: impl Future<Output = RetT> + Unpin,
    mut periodic: impl FnMut(),
) -> RetT {
    loop {
        tokio::select! {
            ret = &mut fut => {
                return ret;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                periodic();
            }
        }
    }
}

pub struct Pool<T> {
    mutex: Mutex<Vec<T>>,
    semaphore: Semaphore,
}

impl<T> Pool<T> {
    pub fn new(slots: NonZeroU32) -> Self {
        Self {
            mutex: Default::default(),
            semaphore: Semaphore::new(usize::try_from(u32::from(slots)).unwrap()),
        }
    }

    /// Run call a closure after obtaining a slot, potentially with a pre-built item.
    ///
    /// The calling thread will block until there is an available slot. Once there is, the provided
    /// closure will be called, potentially with an item returned from a previously called closure.
    /// Upon completion, if the closure returns a new item, the item will be provided to a
    /// subsequent closure.
    pub async fn call_with_item<U, E, F, Fut>(&self, f: F) -> Result<U, E>
    where
        F: FnOnce(Option<T>) -> Fut,
        Fut: Future<Output = Result<(T, U), E>>,
    {
        let _permit = self.semaphore.acquire().await.unwrap();
        let item = self.mutex.lock().unwrap().pop();
        f(item).await.map(|(item, ret)| {
            self.mutex.lock().unwrap().push(item);
            ret
        })
    }
}
