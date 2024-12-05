mod github;
mod tcp_upload;

use anyhow::Result;
use maelstrom_base::Sha256Digest;
use maelstrom_util::r#async::Pool;
use std::future::Future;
use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinSet;

pub use tcp_upload::start_task;

type SuccessCb = Box<dyn FnOnce() + Send + Sync>;

pub struct Message {
    pub path: PathBuf,
    pub digest: Sha256Digest,
    pub success_callback: SuccessCb,
}

pub type Sender = UnboundedSender<Message>;
pub type Receiver = UnboundedReceiver<Message>;

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

pub const MAX_CLIENT_UPLOADS: usize = 10;

fn start_task_inner<PoolItemT, RetFutT, PushOneArtifactFn>(
    join_set: &mut JoinSet<Result<()>>,
    mut receiver: Receiver,
    push_one_artifact: PushOneArtifactFn,
) where
    PoolItemT: Send + 'static,
    RetFutT: Future<Output = Result<(PoolItemT, ())>> + Send,
    PushOneArtifactFn: FnMut(Option<PoolItemT>, PathBuf, Sha256Digest, SuccessCb) -> RetFutT
        + Clone
        + Send
        + 'static,
{
    let pool = Arc::new(Pool::new(
        NonZeroU32::try_from(u32::try_from(MAX_CLIENT_UPLOADS).unwrap()).unwrap(),
    ));
    join_set.spawn(async move {
        // When this join_set gets destroyed, all outstanding artifact pusher tasks will be
        // canceled. That will happen either when our sender is closed, or when our own task is
        // canceled. In either case, it means the process is shutting down.
        //
        // We have to be careful not to let the join_set grow indefinitely. This is why we select!
        // below. We always wait on the join_set's join_next. If it's an actual error, we propagate
        // it, which will immediately cancel all the other outstanding tasks.
        let mut join_set = JoinSet::new();
        loop {
            tokio::select! {
                Some(res) = join_set.join_next() => {
                    res.unwrap()?; // We don't expect JoinErrors.
                },
                res = receiver.recv() => {
                    let Some(Message { path, digest, success_callback } ) = res else { break; };
                    let pool = pool.clone();
                    let mut push_one_artifact = push_one_artifact.clone();
                    join_set.spawn(async move {
                        pool.call_with_item(|stream| {
                            push_one_artifact(
                                stream,
                                path,
                                digest,
                                success_callback,
                            )
                        }).await
                    });
                }
            }
        }
        Ok(())
    });
}

fn construct_upload_name(digest: &Sha256Digest, path: &Path) -> String {
    let digest_string = digest.to_string();
    let short_digest = &digest_string[digest_string.len() - 7..];
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    format!("{short_digest} {file_name}")
}
