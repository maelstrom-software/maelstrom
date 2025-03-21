pub use crate::{
    check_open_file_limit,
    config::CacheDir,
    dispatcher::Message,
    types::{DispatcherReceiver as Receiver, DispatcherSender as Sender},
    Tasks,
};
pub use maelstrom_util::cache::GotArtifact;

use crate::dispatcher::{ArtifactFetcher, BrokerSender};
use anyhow::Result;
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, Slots},
    root::RootBuf,
    sync::EventSender,
};
use slog::Logger;
use tokio::{
    sync::mpsc::{self},
    task::JoinSet,
};

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

#[allow(clippy::too_many_arguments)]
pub fn start_task(
    artifact_fetcher: impl ArtifactFetcher + Send + Sync + 'static,
    broker_sender: impl BrokerSender + Send + Sync + 'static,
    cache_root: RootBuf<CacheDir>,
    cache_size: CacheSize,
    done: EventSender,
    inline_limit: InlineLimit,
    log: &Logger,
    receiver: Receiver,
    sender: Sender,
    slots: Slots,
    tasks: JoinSet<Result<()>>,
) -> Result<Tasks> {
    crate::start_dispatcher_task_common(
        move |_| artifact_fetcher,
        broker_sender,
        cache_size,
        cache_root,
        receiver,
        sender,
        Some(done),
        inline_limit,
        log,
        false, /* log_initial_cache_message_at_info */
        slots,
        tasks,
    )
}
