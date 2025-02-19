pub use crate::{
    check_open_file_limit,
    config::CacheDir,
    dispatcher::Message,
    types::{DispatcherReceiver as Receiver, DispatcherSender as Sender},
};
pub use maelstrom_util::cache::GotArtifact;

use crate::dispatcher::{ArtifactFetcher, BrokerSender};
use anyhow::{Error, Result};
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, Slots},
    root::RootBuf,
};
use slog::Logger;
use tokio::{
    sync::mpsc::{self},
    task::JoinHandle,
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
    inline_limit: InlineLimit,
    log: Logger,
    receiver: Receiver,
    sender: Sender,
    slots: Slots,
) -> Result<JoinHandle<Error>> {
    let args = crate::DispatcherArgs {
        broker_sender,
        cache_size,
        cache_root,
        dispatcher_receiver: receiver,
        dispatcher_sender: sender,
        inline_limit,
        log,
        log_initial_cache_message_at_info: false,
        slots,
    };

    crate::start_dispatcher_task_common(move |_| artifact_fetcher, args)
}
