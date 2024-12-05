mod tcp_upload;

use maelstrom_base::Sha256Digest;
use std::path::PathBuf;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub use tcp_upload::start_task;

pub struct Message {
    pub path: PathBuf,
    pub digest: Sha256Digest,
    pub success_callback: Box<dyn FnOnce() + Send + Sync>,
}

pub type Sender = UnboundedSender<Message>;
pub type Receiver = UnboundedReceiver<Message>;

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

pub const MAX_CLIENT_UPLOADS: usize = 10;
