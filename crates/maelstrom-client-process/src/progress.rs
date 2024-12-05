use maelstrom_client_base::RemoteProgress;
use maelstrom_github::{AzureResult, SeekableStream};
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::HashMap,
    future::Future,
    pin::{pin, Pin},
    sync::OnceLock,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{ready, Poll},
};
use tokio::io::{self, AsyncRead};

pub struct LazyProgress<FactoryT> {
    factory: FactoryT,
    tracker: OnceLock<RunningProgress>,
}

impl<FactoryT> LazyProgress<FactoryT> {
    pub fn new(factory: FactoryT) -> Arc<Self> {
        Arc::new(Self {
            factory,
            tracker: OnceLock::new(),
        })
    }
}

impl<FactoryT> maelstrom_container::ProgressTracker for LazyProgress<FactoryT>
where
    FactoryT: Fn(u64) -> RunningProgress + Send + Sync + Unpin + 'static,
{
    fn set_length(&self, length: u64) {
        self.tracker.set((self.factory)(length)).unwrap();
    }

    fn inc(&self, v: u64) {
        self.tracker.get().unwrap().update(v);
    }
}

#[derive(Clone, Debug)]
pub struct RunningProgress {
    name: String,
    progress: Arc<AtomicU64>,
    tracker: Arc<Mutex<ProgressTrackerInner>>,
}

impl RunningProgress {
    pub fn update(&self, amount_to_add: u64) {
        self.progress.fetch_add(amount_to_add, Ordering::AcqRel);
    }

    pub fn reset(&self) {
        self.progress.store(0, Ordering::Release);
    }
}

impl Drop for RunningProgress {
    fn drop(&mut self) {
        self.tracker
            .lock()
            .unwrap()
            .tasks
            .remove(&self.name)
            .assert_is_some();
    }
}

#[derive(Clone, Default)]
pub struct ProgressTracker {
    inner: Arc<Mutex<ProgressTrackerInner>>,
}

#[derive(Debug, Default)]
struct ProgressTrackerInner {
    tasks: HashMap<String, ProgressTrackerTask>,
}

#[derive(Debug)]
struct ProgressTrackerTask {
    size: u64,
    progress: Arc<AtomicU64>,
}

impl ProgressTracker {
    pub fn new_task(&self, name: impl Into<String>, size: u64) -> RunningProgress {
        let name = name.into();
        let progress = Arc::new(AtomicU64::new(0));
        let task = ProgressTrackerTask {
            size,
            progress: progress.clone(),
        };
        self.inner
            .lock()
            .unwrap()
            .tasks
            .insert(name.clone(), task)
            .assert_is_none();
        RunningProgress {
            name,
            progress,
            tracker: self.inner.clone(),
        }
    }

    pub fn get_remote_progresses(&self) -> Vec<RemoteProgress> {
        self.inner
            .lock()
            .unwrap()
            .tasks
            .iter()
            .map(
                |(name, ProgressTrackerTask { size, progress })| RemoteProgress {
                    name: name.clone(),
                    size: *size,
                    progress: progress.load(Ordering::Acquire),
                },
            )
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct UploadProgressReader<ReadT> {
    prog: RunningProgress,
    read: ReadT,
}

impl<ReadT> UploadProgressReader<ReadT> {
    pub fn new(prog: RunningProgress, read: ReadT) -> Self {
        Self { prog, read }
    }
}

impl<ReadT: AsyncRead + Unpin> AsyncRead for UploadProgressReader<ReadT> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        dst: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let start_len = dst.filled().len();
        let me = self.get_mut();
        let result = AsyncRead::poll_read(pin!(&mut me.read), cx, dst);
        let amount_read = dst.filled().len() - start_len;
        me.prog.update(amount_read as u64);
        result
    }
}

impl<ReadT: futures::io::AsyncRead + Unpin> futures::io::AsyncRead for UploadProgressReader<ReadT> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        dst: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        let size = ready!(futures::io::AsyncRead::poll_read(
            pin!(&mut me.read),
            cx,
            dst
        ))?;
        me.prog.update(size as u64);
        Poll::Ready(Ok(size))
    }
}

impl<ReadT: SeekableStream + Clone> SeekableStream for UploadProgressReader<ReadT> {
    fn reset<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> Pin<Box<dyn Future<Output = AzureResult<()>> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
    {
        self.prog.reset();
        self.read.reset()
    }

    fn len(&self) -> usize {
        self.read.len()
    }
}
