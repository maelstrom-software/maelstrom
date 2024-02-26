use crate::ArtifactPushRequest;
use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{ArtifactPusherToBroker, BrokerToArtifactPusher, Hello},
    Sha256Digest,
};
use maelstrom_client_base::ArtifactUploadProgress;
use maelstrom_util::{config::BrokerAddr, fs::Fs, io::FixedSizeReader, net};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::{
    collections::HashMap,
    io,
    net::TcpStream,
    path::{Path, PathBuf},
    sync::mpsc::Receiver,
    thread::{self},
};

struct UploadProgress {
    size: u64,
    progress: AtomicU64,
}

#[derive(Clone, Default)]
pub struct ArtifactUploadTracker {
    uploads: Arc<Mutex<HashMap<String, Arc<UploadProgress>>>>,
}

impl ArtifactUploadTracker {
    fn new_upload(&self, name: impl Into<String>, size: u64) -> Arc<UploadProgress> {
        let mut uploads = self.uploads.lock().unwrap();
        let prog = Arc::new(UploadProgress {
            size,
            progress: AtomicU64::new(0),
        });
        uploads.insert(name.into(), prog.clone());
        prog
    }

    fn remove_upload(&self, name: &str) {
        let mut uploads = self.uploads.lock().unwrap();
        uploads.remove(name);
    }

    pub fn get_artifact_upload_progress(&self) -> Vec<ArtifactUploadProgress> {
        let uploads = self.uploads.lock().unwrap();
        uploads
            .iter()
            .map(|(name, p)| ArtifactUploadProgress {
                name: name.clone(),
                size: p.size,
                progress: p.progress.load(Ordering::Acquire),
            })
            .collect()
    }
}

struct UploadProgressReader<ReadT> {
    prog: Arc<UploadProgress>,
    read: ReadT,
}

impl<ReadT> UploadProgressReader<ReadT> {
    fn new(prog: Arc<UploadProgress>, read: ReadT) -> Self {
        Self { prog, read }
    }
}

impl<ReadT: io::Read> io::Read for UploadProgressReader<ReadT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let amount_read = self.read.read(buf)?;
        self.prog
            .progress
            .fetch_add(amount_read as u64, Ordering::AcqRel);
        Ok(amount_read)
    }
}

fn construct_upload_name(digest: &Sha256Digest, path: &Path) -> String {
    let digest_string = digest.to_string();
    let short_digest = &digest_string[digest_string.len() - 7..];
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    format!("{short_digest} {file_name}")
}

fn push_one_artifact(
    upload_tracker: ArtifactUploadTracker,
    broker_addr: BrokerAddr,
    path: PathBuf,
    digest: Sha256Digest,
) -> Result<()> {
    let mut stream = TcpStream::connect(broker_addr.inner())?;
    net::write_message_to_socket(&mut stream, Hello::ArtifactPusher)?;

    let fs = Fs::new();
    let file = fs.open_file(&path)?;
    let size = file.metadata()?.len();

    let upload_name = construct_upload_name(&digest, &path);
    let prog = upload_tracker.new_upload(&upload_name, size);

    let mut file = UploadProgressReader::new(prog, FixedSizeReader::new(file, size));

    net::write_message_to_socket(&mut stream, ArtifactPusherToBroker(digest, size))?;
    let copied = io::copy(&mut file, &mut stream)?;
    assert_eq!(copied, size);

    let BrokerToArtifactPusher(resp) = net::read_message_from_socket(&mut stream)?;

    upload_tracker.remove_upload(&upload_name);
    resp.map_err(|e| anyhow!("Error from broker: {e}"))
}

pub struct ArtifactPusher {
    broker_addr: BrokerAddr,
    receiver: Receiver<ArtifactPushRequest>,
    upload_tracker: ArtifactUploadTracker,
}

impl ArtifactPusher {
    pub fn new(
        broker_addr: BrokerAddr,
        receiver: Receiver<ArtifactPushRequest>,
        upload_tracker: ArtifactUploadTracker,
    ) -> Self {
        Self {
            broker_addr,
            receiver,
            upload_tracker,
        }
    }

    /// Processes one request. In order to drive the ArtifactPusher, this should be called in a loop
    /// until the function return false
    pub fn process_one<'a, 'b>(&mut self, scope: &'a thread::Scope<'b, '_>) -> bool
    where
        'a: 'b,
    {
        if let Ok(msg) = self.receiver.recv() {
            let upload_tracker = self.upload_tracker.clone();
            let broker_addr = self.broker_addr;
            // N.B. We are ignoring this Result<_>
            scope.spawn(move || {
                push_one_artifact(upload_tracker, broker_addr, msg.path, msg.digest)
            });
            true
        } else {
            false
        }
    }
}
