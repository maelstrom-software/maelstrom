pub use crate::{
    check_open_file_limit,
    config::CacheDir,
    dispatcher::{ArtifactFetcher, BrokerSender, Message},
    types::{DispatcherReceiver as Receiver, DispatcherSender as Sender},
};
pub use maelstrom_util::cache::GotArtifact;

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
    dispatcher_receiver: Receiver,
    dispatcher_sender: Sender,
    log: &Logger,
) -> Result<JoinHandle<Error>> {
    crate::start_dispatcher_task_common(
        move |_| artifact_fetcher,
        broker_sender,
        config.cache_size,
        config.cache_root,
        dispatcher_receiver,
        dispatcher_sender,
        config.inline_limit,
        log,
        false,
        config.slots,
    )
}
