pub use crate::{
    check_open_file_limit,
    config::CacheDir,
    dispatcher::{ArtifactFetcher, BrokerSender, Message},
    types::{DispatcherReceiver as Receiver, DispatcherSender as Sender},
};
pub use maelstrom_util::cache::GotArtifact;

use crate::{
    dispatcher::Dispatcher,
    dispatcher_adapter::DispatcherAdapter,
    executor::{MountDir, TmpfsDir},
};
use anyhow::Result;
use maelstrom_layer_fs::BlobDir;
use maelstrom_util::{
    cache::{self, fs::std::Fs, Cache},
    config::common::{CacheSize, InlineLimit, Slots},
    root::RootBuf,
};
use slog::{debug, Logger};
use tokio::{
    sync::mpsc::{self},
    task::{self, JoinHandle},
};

pub struct Config {
    pub cache_root: RootBuf<CacheDir>,
    pub cache_size: CacheSize,
    pub inline_limit: InlineLimit,
    pub slots: Slots,
}

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

pub fn start_task(
    artifact_fetcher: impl ArtifactFetcher + Send + Sync + 'static,
    broker_sender: impl BrokerSender + Send + Sync + 'static,
    config: Config,
    mut dispatcher_receiver: Receiver,
    dispatcher_sender: Sender,
    log: &Logger,
) -> Result<JoinHandle<()>> {
    let mount_dir = config.cache_root.join::<MountDir>("mount");
    let tmpfs_dir = config.cache_root.join::<TmpfsDir>("upper");
    let cache_root = config.cache_root.join::<cache::CacheDir>("artifacts");
    let blob_dir = cache_root.join::<BlobDir>("sha256/blob");

    // Create the local_worker's cache. This is the same cache as the "real" worker uses, except we
    // will be populating it with symlinks in certain cases.
    let (local_worker_cache, local_worker_temp_file_factory) =
        Cache::new(Fs, cache_root, config.cache_size, log.clone(), false)?;

    // Create the local_worker's deps. This the same adapter as the "real" worker uses.
    let local_worker_dispatcher_adapter = DispatcherAdapter::new(
        dispatcher_sender,
        config.inline_limit,
        log.clone(),
        mount_dir,
        tmpfs_dir,
        blob_dir,
        local_worker_temp_file_factory,
    )?;

    // Create the actual local_worker.
    let mut worker_dispatcher = Dispatcher::new(
        local_worker_dispatcher_adapter,
        artifact_fetcher,
        broker_sender,
        local_worker_cache,
        config.slots,
    );

    let handle_worker_message = |msg, worker: &mut Dispatcher<_, _, _, _>| -> Result<()> {
        if let Message::Shutdown(error) = msg {
            Err(error)
        } else {
            worker.receive_message(msg);
            Ok(())
        }
    };

    // Spawn a task for the local_worker.
    let log_clone = log.clone();
    Ok(task::spawn(async move {
        let shutdown_error = loop {
            let msg = dispatcher_receiver.recv().await.expect("missing shutdown");
            if let Err(err) = handle_worker_message(msg, &mut worker_dispatcher) {
                break err;
            }
        };

        debug!(
            log_clone,
            "shutting down local worker due to {shutdown_error}"
        );
        worker_dispatcher.receive_message(Message::Shutdown(shutdown_error));
        debug!(
            log_clone,
            "canceling {} running jobs",
            worker_dispatcher.num_jobs_executing()
        );

        while worker_dispatcher.num_jobs_executing() > 0 {
            let msg = dispatcher_receiver.recv().await.expect("missing shutdown");
            let _ = handle_worker_message(msg, &mut worker_dispatcher);
        }

        debug!(log_clone, "local worker exiting");
    }))
}
