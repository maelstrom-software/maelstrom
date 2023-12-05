use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use indicatif::ProgressBar;
use meticulous_base::{
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    stats::JobStateCounts,
    ClientJobId, JobResult, JobSpec, NonEmpty, Sha256Digest,
};
use meticulous_container::ContainerImageDepot;
use meticulous_util::{config::BrokerAddr, ext::OptionExt as _, fs::Fs, io::FixedSizeReader, net};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, DisplayFromStr};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::{self, JoinHandle},
    time::SystemTime,
};

fn push_one_artifact(broker_addr: BrokerAddr, path: PathBuf, digest: Sha256Digest) -> Result<()> {
    let fs = Fs::new();
    let mut stream = TcpStream::connect(broker_addr.inner())?;
    let file = fs.open_file(path)?;
    let size = file.metadata()?.len();
    let mut file = FixedSizeReader::new(file, size);
    net::write_message_to_socket(&mut stream, Hello::ArtifactPusher)?;
    net::write_message_to_socket(&mut stream, ArtifactPusherToBroker(digest, size))?;
    let copied = io::copy(&mut file, &mut stream)?;
    assert_eq!(copied, size);
    let BrokerToArtifactPusher(resp) = net::read_message_from_socket(&mut stream)?;
    resp.map_err(|e| anyhow!("Error from broker: {e}"))
}

fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = Fs::new();
    let mut hasher = Sha256::new();
    if path.extension() != Some("tar".as_ref()) {
        return Err(anyhow!(
            "path \"{}\" does not end in \".tar\"",
            path.display()
        ));
    }
    let mut f = fs.open_file(path)?;
    std::io::copy(&mut f, &mut hasher)?;
    let mtime = f.metadata()?.modified()?;

    Ok((mtime, Sha256Digest::new(hasher.finalize().into())))
}

enum DispatcherMessage {
    BrokerToClient(BrokerToClient),
    AddArtifact(PathBuf, Sha256Digest),
    AddJob(JobSpec, JobResponseHandler),
    GetJobStateCounts(SyncSender<JobStateCounts>),
    Stop,
}

struct ArtifactPushRequest {
    path: PathBuf,
    digest: Sha256Digest,
}

struct ArtifactPusher {
    broker_addr: BrokerAddr,
    receiver: Receiver<ArtifactPushRequest>,
}

impl ArtifactPusher {
    fn new(broker_addr: BrokerAddr, receiver: Receiver<ArtifactPushRequest>) -> Self {
        Self {
            broker_addr,
            receiver,
        }
    }

    /// Processes one request. In order to drive the ArtifactPusher, this should be called in a loop
    /// until the function return false
    fn process_one<'a, 'b>(&mut self, scope: &'a thread::Scope<'b, '_>) -> bool
    where
        'a: 'b,
    {
        if let Ok(msg) = self.receiver.recv() {
            let broker_addr = self.broker_addr;
            // N.B. We are ignoring this Result<_>
            scope.spawn(move || push_one_artifact(broker_addr, msg.path, msg.digest));
            true
        } else {
            false
        }
    }
}

struct Dispatcher {
    receiver: Receiver<DispatcherMessage>,
    stream: TcpStream,
    artifact_pusher: SyncSender<ArtifactPushRequest>,
    stop_when_all_completed: bool,
    next_client_job_id: u32,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    handlers: HashMap<ClientJobId, JobResponseHandler>,
    stats_reqs: VecDeque<SyncSender<JobStateCounts>>,
}

impl Dispatcher {
    fn new(
        receiver: Receiver<DispatcherMessage>,
        stream: TcpStream,
        artifact_pusher: SyncSender<ArtifactPushRequest>,
    ) -> Self {
        Self {
            receiver,
            stream,
            artifact_pusher,
            stop_when_all_completed: false,
            next_client_job_id: 0u32,
            artifacts: Default::default(),
            handlers: Default::default(),
            stats_reqs: Default::default(),
        }
    }

    /// Processes one request. In order to drive the dispatcher, this should be called in a loop
    /// until the function return false
    fn process_one(&mut self) -> Result<bool> {
        let msg = self.receiver.recv()?;
        match msg {
            DispatcherMessage::BrokerToClient(BrokerToClient::JobResponse(cjid, result)) => {
                self.handlers.remove(&cjid).unwrap()(cjid, result)?;
                if self.stop_when_all_completed && self.handlers.is_empty() {
                    return Ok(false);
                }
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::TransferArtifact(digest)) => {
                let path = self.artifacts.get(&digest).unwrap().clone();
                self.artifact_pusher
                    .send(ArtifactPushRequest { path, digest })?;
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests")
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::JobStateCountsResponse(res)) => {
                self.stats_reqs.pop_front().unwrap().send(res).ok();
            }
            DispatcherMessage::AddArtifact(path, digest) => {
                self.artifacts.insert(digest, path);
            }
            DispatcherMessage::AddJob(spec, handler) => {
                let cjid = self.next_client_job_id.into();
                self.handlers.insert(cjid, handler).assert_is_none();
                self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();
                net::write_message_to_socket(
                    &mut self.stream,
                    ClientToBroker::JobRequest(cjid, spec),
                )?;
            }
            DispatcherMessage::Stop => {
                if self.handlers.is_empty() {
                    return Ok(false);
                }
                self.stop_when_all_completed = true;
            }
            DispatcherMessage::GetJobStateCounts(sender) => {
                net::write_message_to_socket(
                    &mut self.stream,
                    ClientToBroker::JobStateCountsRequest,
                )?;
                self.stats_reqs.push_back(sender);
            }
        }
        Ok(true)
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum DigestRepositoryVersion {
    #[default]
    V0 = 0,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
struct DigestRepositoryEntry {
    #[serde_as(as = "DisplayFromStr")]
    digest: Sha256Digest,
    mtime: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Default)]
struct DigestRepositoryContents {
    pub version: DigestRepositoryVersion,
    pub digests: HashMap<PathBuf, DigestRepositoryEntry>,
}

impl DigestRepositoryContents {
    fn from_str(s: &str) -> Result<Self> {
        Ok(toml::from_str(s)?)
    }

    fn to_pretty_string(&self) -> String {
        toml::to_string_pretty(self).unwrap()
    }
}

const CACHED_IMAGE_FILE_NAME: &str = "meticulous-cached-digests.toml";

struct DigestRespository {
    fs: Fs,
    path: PathBuf,
}

impl DigestRespository {
    fn new(path: &Path) -> Self {
        Self {
            fs: Fs::new(),
            path: path.into(),
        }
    }

    fn add(&self, path: PathBuf, mtime: SystemTime, digest: Sha256Digest) -> Result<()> {
        self.fs.create_dir_all(&self.path)?;
        let mut file = self
            .fs
            .open_or_create_file(self.path.join(CACHED_IMAGE_FILE_NAME))?;
        file.lock_exclusive()?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut digests = DigestRepositoryContents::from_str(&contents).unwrap_or_default();
        digests.digests.insert(
            path,
            DigestRepositoryEntry {
                mtime: mtime.into(),
                digest,
            },
        );

        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.write_all(digests.to_pretty_string().as_bytes())?;

        Ok(())
    }

    fn get(&self, path: &PathBuf) -> Result<Option<Sha256Digest>> {
        let Some(contents) = self
            .fs
            .read_to_string_if_exists(self.path.join(CACHED_IMAGE_FILE_NAME))?
        else {
            return Ok(None);
        };
        let mut digests = DigestRepositoryContents::from_str(&contents).unwrap_or_default();
        let Some(entry) = digests.digests.remove(path) else {
            return Ok(None);
        };
        let current_mtime: DateTime<Utc> = self.fs.metadata(path)?.modified()?.into();
        Ok((current_mtime == entry.mtime).then_some(entry.digest))
    }
}

#[test]
fn digest_repository_simple_add_get() {
    let fs = Fs::new();
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = DigestRespository::new(tmp_dir.path());

    let foo_path = tmp_dir.path().join("foo.tar");
    fs.write(&foo_path, "foo").unwrap();
    let (mtime, digest) = calculate_digest(&foo_path).unwrap();
    repo.add(foo_path.clone(), mtime, digest.clone()).unwrap();

    assert_eq!(repo.get(&foo_path).unwrap(), Some(digest));
}

#[test]
fn digest_repository_simple_add_get_after_modify() {
    let fs = Fs::new();
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = DigestRespository::new(tmp_dir.path());

    let foo_path = tmp_dir.path().join("foo.tar");
    fs.write(&foo_path, "foo").unwrap();
    let (mtime, digest) = calculate_digest(&foo_path).unwrap();
    repo.add(foo_path.clone(), mtime, digest.clone()).unwrap();

    // apparently depending on the file-system mtime can have up to a 10ms granularity
    std::thread::sleep(std::time::Duration::from_millis(20));
    fs.write(&foo_path, "bar").unwrap();

    assert_eq!(repo.get(&foo_path).unwrap(), None);
}

struct SocketReader {
    stream: TcpStream,
    channel: SyncSender<DispatcherMessage>,
}

impl SocketReader {
    fn new(stream: TcpStream, channel: SyncSender<DispatcherMessage>) -> Self {
        Self { stream, channel }
    }

    fn process_one(&mut self) -> bool {
        let Ok(msg) = net::read_message_from_socket(&mut self.stream) else {
            return false;
        };
        self.channel
            .send(DispatcherMessage::BrokerToClient(msg))
            .is_ok()
    }
}

struct ClientDeps {
    dispatcher: Dispatcher,
    artifact_pusher: ArtifactPusher,
    socket_reader: SocketReader,
    dispatcher_sender: SyncSender<DispatcherMessage>,
}

impl ClientDeps {
    fn new(broker_addr: BrokerAddr) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr.inner())?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let (dispatcher_sender, dispatcher_receiver) = mpsc::sync_channel(1000);
        let (artifact_send, artifact_recv) = mpsc::sync_channel(1000);
        let stream_clone = stream.try_clone()?;
        Ok(Self {
            dispatcher: Dispatcher::new(dispatcher_receiver, stream_clone, artifact_send),
            artifact_pusher: ArtifactPusher::new(broker_addr, artifact_recv),
            socket_reader: SocketReader::new(stream, dispatcher_sender.clone()),
            dispatcher_sender,
        })
    }

    fn run_on_thread(mut self) -> JoinHandle<Result<()>> {
        thread::spawn(move || {
            thread::scope(|scope| {
                let dispatcher_handle = scope.spawn(move || {
                    while self.dispatcher.process_one()? {}
                    self.dispatcher.stream.shutdown(std::net::Shutdown::Both)?;
                    Ok(())
                });
                scope.spawn(move || while self.artifact_pusher.process_one(scope) {});
                scope.spawn(move || while self.socket_reader.process_one() {});
                dispatcher_handle.join().unwrap()
            })
        })
    }
}

pub type JobResponseHandler = Box<dyn FnOnce(ClientJobId, JobResult) -> Result<()> + Send + Sync>;

pub struct Client {
    dispatcher_sender: SyncSender<DispatcherMessage>,
    deps_handle: JoinHandle<Result<()>>,
    digest_repo: DigestRespository,
    container_image_depot: ContainerImageDepot,
    digest_to_container_env: HashMap<NonEmpty<Sha256Digest>, Vec<String>>,
    processed_artifact_paths: HashSet<PathBuf>,
}

impl Client {
    pub fn new(
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let deps = ClientDeps::new(broker_addr)?;
        let dispatcher_sender = deps.dispatcher_sender.clone();
        let deps_handle = deps.run_on_thread();

        Ok(Client {
            dispatcher_sender,
            deps_handle,
            digest_repo: DigestRespository::new(cache_dir.as_ref()),
            container_image_depot: ContainerImageDepot::new(project_dir.as_ref())?,
            digest_to_container_env: HashMap::default(),
            processed_artifact_paths: HashSet::default(),
        })
    }

    pub fn add_artifact(&mut self, path: &Path) -> Result<Sha256Digest> {
        let fs = Fs::new();
        let path = fs.canonicalize(path)?;
        let digest = if let Some(digest) = self.digest_repo.get(&path)? {
            digest
        } else {
            let (mtime, digest) = calculate_digest(&path)?;
            self.digest_repo.add(path.clone(), mtime, digest.clone())?;
            digest
        };
        if !self.processed_artifact_paths.contains(&path) {
            self.dispatcher_sender
                .send(DispatcherMessage::AddArtifact(path.clone(), digest.clone()))?;
            self.processed_artifact_paths.insert(path);
        }
        Ok(digest)
    }

    pub fn add_container(
        &mut self,
        pkg: &str,
        version: &str,
        prog: Option<ProgressBar>,
    ) -> Result<NonEmpty<Sha256Digest>> {
        let prog = prog.unwrap_or_else(ProgressBar::hidden);
        let img = self
            .container_image_depot
            .get_container_image(pkg, version, prog)?;
        let env = img.env().cloned();
        let digests = NonEmpty::<PathBuf>::try_from(img.layers)
            .map_err(|_| anyhow!("empty layer vector for {pkg}:{version}"))?
            .try_map(|layer| self.add_artifact(&layer))?;
        if let Some(env) = env {
            self.digest_to_container_env.insert(digests.clone(), env);
        }
        Ok(digests)
    }

    fn maybe_add_container_environment(&mut self, spec: &mut JobSpec) {
        for (digests, env) in &self.digest_to_container_env {
            let container_digests: HashSet<_> = digests.iter().collect();
            let job_digests: HashSet<_> = spec.layers.iter().collect();
            if job_digests.is_superset(&container_digests) {
                spec.environment.extend(env.iter().cloned());
            }
        }
    }

    pub fn add_job(&mut self, mut spec: JobSpec, handler: JobResponseHandler) {
        self.maybe_add_container_environment(&mut spec);

        // We will only get an error if the dispatcher has closed its receiver, which will only
        // happen if it ran into an error. We'll get that error when we wait in
        // `wait_for_oustanding_job`.
        let _ = self
            .dispatcher_sender
            .send(DispatcherMessage::AddJob(spec, handler));
    }

    pub fn wait_for_outstanding_jobs(self) -> Result<()> {
        self.dispatcher_sender.send(DispatcherMessage::Stop)?;
        self.deps_handle.join().unwrap()?;
        Ok(())
    }

    pub fn get_job_state_counts(&mut self) -> Result<JobStateCounts> {
        let (sender, recv) = mpsc::sync_channel(1);
        self.dispatcher_sender
            .send(DispatcherMessage::GetJobStateCounts(sender))?;
        Ok(recv.recv()?)
    }
}
