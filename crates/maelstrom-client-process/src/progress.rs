use maelstrom_client_base::RemoteProgress;
use std::collections::HashMap;
use std::pin::{pin, Pin};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::io::{self, AsyncRead};

pub struct RunningProgress {
    size: AtomicU64,
    progress: AtomicU64,
}

impl maelstrom_container::ProgressTracker for RunningProgress {
    fn set_length(&self, length: u64) {
        self.size.store(length, Ordering::Release);
    }

    fn inc(&self, v: u64) {
        self.progress.fetch_add(v, Ordering::AcqRel);
    }
}

#[derive(Clone, Default)]
pub struct ProgressTracker {
    tasks: Arc<Mutex<HashMap<String, Arc<RunningProgress>>>>,
}

impl ProgressTracker {
    pub fn new_task(&self, name: impl Into<String>, size: u64) -> Arc<RunningProgress> {
        let mut tasks = self.tasks.lock().unwrap();
        let prog = Arc::new(RunningProgress {
            size: AtomicU64::new(size),
            progress: AtomicU64::new(0),
        });
        tasks.insert(name.into(), prog.clone());
        prog
    }

    pub fn remove_task(&self, name: &str) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.remove(name);
    }

    pub fn get_remote_progresses(&self) -> Vec<RemoteProgress> {
        let tasks = self.tasks.lock().unwrap();
        tasks
            .iter()
            .map(|(name, p)| RemoteProgress {
                name: name.clone(),
                size: p.size.load(Ordering::Acquire),
                progress: p.progress.load(Ordering::Acquire),
            })
            .collect()
    }
}

pub struct UploadProgressReader<ReadT> {
    prog: Arc<RunningProgress>,
    read: ReadT,
}

impl<ReadT> UploadProgressReader<ReadT> {
    pub fn new(prog: Arc<RunningProgress>, read: ReadT) -> Self {
        Self { prog, read }
    }
}

impl<ReadT: AsyncRead + Unpin> AsyncRead for UploadProgressReader<ReadT> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        dst: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let start_len = dst.filled().len();
        let me = self.get_mut();
        let result = AsyncRead::poll_read(pin!(&mut me.read), cx, dst);
        let amount_read = dst.filled().len() - start_len;
        me.prog
            .progress
            .fetch_add(amount_read as u64, Ordering::AcqRel);
        result
    }
}
