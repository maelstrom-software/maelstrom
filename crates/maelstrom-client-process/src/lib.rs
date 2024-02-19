mod rpc;
mod test;

use anyhow::{anyhow, Context as _, Result};
use chrono::{DateTime, Utc};
use itertools::Itertools as _;
use maelstrom_base::{
    manifest::{
        ManifestEntry, ManifestEntryData, ManifestEntryMetadata, ManifestWriter, Mode,
        UnixTimestamp,
    },
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    stats::JobStateCounts,
    ArtifactType, ClientJobId, JobSpec, Sha256Digest, Utf8Path, Utf8PathBuf,
};
use maelstrom_client_base::{
    spec::{Layer, PrefixOptions, SymlinkSpec},
    ClientDriverMode, JobResponseHandler, MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{ContainerImage, ContainerImageDepot, ProgressTracker};
use maelstrom_util::{
    config::BrokerAddr, ext::OptionExt as _, fs::Fs, io::FixedSizeReader,
    manifest::ManifestBuilder, net,
};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, DisplayFromStr};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::{self, JoinHandle},
    time::SystemTime,
};
use test::client_driver::SingleThreadedClientDriver;

fn new_driver(mode: ClientDriverMode) -> Box<dyn ClientDriver + Send + Sync> {
    match mode {
        ClientDriverMode::MultiThreaded => Box::<MultiThreadedClientDriver>::default(),
        ClientDriverMode::SingleThreaded => Box::<SingleThreadedClientDriver>::default(),
    }
}

fn push_one_artifact(broker_addr: BrokerAddr, path: PathBuf, digest: Sha256Digest) -> Result<()> {
    let mut stream = TcpStream::connect(broker_addr.inner())?;
    net::write_message_to_socket(&mut stream, Hello::ArtifactPusher)?;

    let fs = Fs::new();
    let file = fs.open_file(path)?;
    let size = file.metadata()?.len();
    let mut file = FixedSizeReader::new(file, size);

    net::write_message_to_socket(&mut stream, ArtifactPusherToBroker(digest, size))?;
    let copied = io::copy(&mut file, &mut stream)?;
    assert_eq!(copied, size);
    let BrokerToArtifactPusher(resp) = net::read_message_from_socket(&mut stream)?;
    resp.map_err(|e| anyhow!("Error from broker: {e}"))
}

fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = Fs::new();
    let mut hasher = Sha256::new();
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
        self.handle_message(msg)
    }

    fn try_process_one(&mut self) -> Result<bool> {
        let msg = self.receiver.try_recv()?;
        self.handle_message(msg)
    }

    fn handle_message(&mut self, msg: DispatcherMessage) -> Result<bool> {
        match msg {
            DispatcherMessage::BrokerToClient(BrokerToClient::JobResponse(cjid, result)) => {
                self.handlers.remove(&cjid).unwrap()(cjid, result);
                if self.stop_when_all_completed && self.handlers.is_empty() {
                    return Ok(false);
                }
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::TransferArtifact(digest)) => {
                let path = self
                    .artifacts
                    .get(&digest)
                    .unwrap_or_else(|| {
                        panic!("got request for unknown artifact with digest {digest}")
                    })
                    .clone();
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
enum DigestRepositoryVersion {
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
    version: DigestRepositoryVersion,
    digests: HashMap<PathBuf, DigestRepositoryEntry>,
}

impl DigestRepositoryContents {
    fn from_str(s: &str) -> Result<Self> {
        Ok(toml::from_str(s)?)
    }

    fn to_pretty_string(&self) -> String {
        toml::to_string_pretty(self).unwrap()
    }
}

const CACHED_IMAGE_FILE_NAME: &str = "maelstrom-cached-digests.toml";

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
        let mut stream = TcpStream::connect(broker_addr.inner())
            .with_context(|| format!("failed to connect to {broker_addr}"))?;
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
}

trait ClientDriver {
    fn drive(&mut self, deps: ClientDeps);
    fn stop(&mut self) -> Result<()>;

    fn process_broker_msg_single_threaded(&self, _count: usize) {
        unimplemented!()
    }

    fn process_client_messages_single_threaded(&self) {
        unimplemented!()
    }

    fn process_artifact_single_threaded(&self) {
        unimplemented!()
    }
}

#[derive(Default)]
struct MultiThreadedClientDriver {
    handle: Option<JoinHandle<Result<()>>>,
}

impl ClientDriver for MultiThreadedClientDriver {
    fn drive(&mut self, mut deps: ClientDeps) {
        assert!(self.handle.is_none());
        self.handle = Some(thread::spawn(move || {
            thread::scope(|scope| {
                let dispatcher_handle = scope.spawn(move || {
                    while deps.dispatcher.process_one()? {}
                    deps.dispatcher.stream.shutdown(std::net::Shutdown::Both)?;
                    Ok(())
                });
                scope.spawn(move || while deps.artifact_pusher.process_one(scope) {});
                scope.spawn(move || while deps.socket_reader.process_one() {});
                dispatcher_handle.join().unwrap()
            })
        }));
    }

    fn stop(&mut self) -> Result<()> {
        self.handle.take().unwrap().join().unwrap()
    }
}

#[derive(Default)]
struct PathHasher {
    hasher: Sha256,
}

impl PathHasher {
    fn new() -> Self {
        Self::default()
    }

    fn hash_path(&mut self, path: &Utf8Path) {
        self.hasher.update(path.as_str().as_bytes());
    }

    fn finish(self) -> Sha256Digest {
        Sha256Digest::new(self.hasher.finalize().into())
    }
}

fn calculate_manifest_entry_path(
    path: &Utf8Path,
    root: &Path,
    prefix_options: &PrefixOptions,
) -> Result<Utf8PathBuf> {
    let mut path = path.to_owned();
    if prefix_options.canonicalize {
        let mut input = path.into_std_path_buf();
        if input.is_relative() {
            input = root.join(input);
        }
        path = Utf8PathBuf::try_from(input.canonicalize()?)?;
    }
    if let Some(prefix) = &prefix_options.strip_prefix {
        if let Ok(new_path) = path.strip_prefix(prefix) {
            path = new_path.to_owned();
        }
    }
    if let Some(prefix) = &prefix_options.prepend_prefix {
        if path.is_absolute() {
            path = prefix.join(path.strip_prefix("/").unwrap());
        } else {
            path = prefix.join(path);
        }
    }
    Ok(path)
}

fn expand_braces(expr: &str) -> Result<Vec<String>> {
    if expr.contains('{') {
        bracoxide::explode(expr).map_err(|e| anyhow!("{e}"))
    } else {
        Ok(vec![expr.to_owned()])
    }
}

/// Having some deterministic time-stamp for files we create in manifests is useful for testing and
/// potentially caching.
/// I picked this time arbitrarily 2024-1-11 11:11:11
const ARBITRARY_TIME: UnixTimestamp = UnixTimestamp(1705000271);

struct Client {
    dispatcher_sender: SyncSender<DispatcherMessage>,
    driver: Box<dyn ClientDriver + Send + Sync>,
    digest_repo: DigestRespository,
    container_image_depot: ContainerImageDepot,
    processed_artifact_paths: HashSet<PathBuf>,
    cache_dir: PathBuf,
    project_dir: PathBuf,
    cached_layers: HashMap<Layer, (Sha256Digest, ArtifactType)>,
}

impl Client {
    fn new(
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let mut driver = new_driver(driver_mode);
        let deps = ClientDeps::new(broker_addr)?;
        let dispatcher_sender = deps.dispatcher_sender.clone();
        driver.drive(deps);

        let fs = Fs::new();
        for d in [MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR] {
            fs.create_dir_all(cache_dir.as_ref().join(d))?;
        }

        Ok(Client {
            dispatcher_sender,
            driver,
            digest_repo: DigestRespository::new(cache_dir.as_ref()),
            container_image_depot: ContainerImageDepot::new(project_dir.as_ref())?,
            processed_artifact_paths: HashSet::default(),
            cache_dir: cache_dir.as_ref().to_owned(),
            project_dir: project_dir.as_ref().to_owned(),
            cached_layers: HashMap::new(),
        })
    }

    fn add_artifact(&mut self, path: &Path) -> Result<Sha256Digest> {
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

    fn build_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        self.cache_dir
            .join(MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    fn build_stub_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        self.cache_dir
            .join(STUB_MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    fn build_symlink_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        self.cache_dir
            .join(SYMLINK_MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    fn build_manifest(
        &mut self,
        paths: impl Iterator<Item = Result<impl AsRef<Path>>>,
        prefix_options: PrefixOptions,
    ) -> Result<PathBuf> {
        let fs = Fs::new();
        let project_dir = self.project_dir.clone();
        let tmp_file_path = self.build_manifest_path(&".temp");
        let manifest_file = fs.create_file(&tmp_file_path)?;
        let data_upload = |path: &_| self.add_artifact(path);
        let mut builder =
            ManifestBuilder::new(manifest_file, false /* follow_symlinks */, data_upload)?;
        let mut path_hasher = PathHasher::new();
        for maybe_path in paths {
            let mut path = maybe_path?.as_ref().to_owned();
            let input_path_relative = path.is_relative();
            if input_path_relative {
                path = project_dir.join(path);
            }
            let utf8_path = Utf8Path::from_path(&path).ok_or_else(|| anyhow!("non-utf8 path"))?;
            path_hasher.hash_path(utf8_path);

            let entry_path = if input_path_relative {
                utf8_path.strip_prefix(&project_dir).unwrap()
            } else {
                utf8_path
            };
            let dest = calculate_manifest_entry_path(entry_path, &project_dir, &prefix_options)?;
            builder.add_file(utf8_path, dest)?;
        }
        drop(builder);

        let manifest_path = self.build_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path)?;
        Ok(manifest_path)
    }

    fn build_stub_manifest(&mut self, stubs: Vec<String>) -> Result<PathBuf> {
        let fs = Fs::new();
        let tmp_file_path = self.build_manifest_path(&".temp");
        let mut writer = ManifestWriter::new(fs.create_file(&tmp_file_path)?)?;
        let mut path_hasher = PathHasher::new();
        for maybe_stub in stubs.iter().map(|s| expand_braces(s)).flatten_ok() {
            let stub = Utf8PathBuf::from(maybe_stub?);
            path_hasher.hash_path(&stub);
            let is_dir = stub.as_str().ends_with('/');
            let data = if is_dir {
                ManifestEntryData::Directory
            } else {
                ManifestEntryData::File(None)
            };
            let metadata = ManifestEntryMetadata {
                size: 0,
                mode: Mode(0o444 | if is_dir { 0o111 } else { 0 }),
                mtime: ARBITRARY_TIME,
            };
            let entry = ManifestEntry {
                path: stub,
                metadata,
                data,
            };
            writer.write_entry(&entry)?;
        }

        let manifest_path = self.build_stub_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path)?;
        Ok(manifest_path)
    }

    fn build_symlink_manifest(&mut self, symlinks: Vec<SymlinkSpec>) -> Result<PathBuf> {
        let fs = Fs::new();
        let tmp_file_path = self.build_manifest_path(&".temp");
        let mut writer = ManifestWriter::new(fs.create_file(&tmp_file_path)?)?;
        let mut path_hasher = PathHasher::new();
        for SymlinkSpec { link, target } in symlinks {
            path_hasher.hash_path(&link);
            path_hasher.hash_path(&target);
            let data = ManifestEntryData::Symlink(target.into_string().into_bytes());
            let metadata = ManifestEntryMetadata {
                size: 0,
                mode: Mode(0o444),
                mtime: ARBITRARY_TIME,
            };
            let entry = ManifestEntry {
                path: link,
                metadata,
                data,
            };
            writer.write_entry(&entry)?;
        }

        let manifest_path = self.build_symlink_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path)?;
        Ok(manifest_path)
    }

    fn add_layer(&mut self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        if let Some(l) = self.cached_layers.get(&layer) {
            return Ok(l.clone());
        }

        let res = match layer.clone() {
            Layer::Tar { path } => (self.add_artifact(path.as_std_path())?, ArtifactType::Tar),
            Layer::Paths {
                paths,
                prefix_options,
            } => {
                let manifest_path = self.build_manifest(paths.iter().map(Ok), prefix_options)?;
                (self.add_artifact(&manifest_path)?, ArtifactType::Manifest)
            }
            Layer::Glob {
                glob,
                prefix_options,
            } => {
                let mut glob_builder = globset::GlobSet::builder();
                glob_builder.add(globset::Glob::new(&glob)?);
                let fs = Fs::new();
                let project_dir = self.project_dir.clone();
                let manifest_path = self.build_manifest(
                    fs.glob_walk(&self.project_dir, &glob_builder.build()?)
                        .map(|p| p.map(|p| p.strip_prefix(&project_dir).unwrap().to_owned())),
                    prefix_options,
                )?;
                (self.add_artifact(&manifest_path)?, ArtifactType::Manifest)
            }
            Layer::Stubs { stubs } => {
                let manifest_path = self.build_stub_manifest(stubs)?;
                (self.add_artifact(&manifest_path)?, ArtifactType::Manifest)
            }
            Layer::Symlinks { symlinks } => {
                let manifest_path = self.build_symlink_manifest(symlinks)?;
                (self.add_artifact(&manifest_path)?, ArtifactType::Manifest)
            }
        };

        self.cached_layers.insert(layer, res.clone());
        Ok(res)
    }

    fn get_container_image(
        &mut self,
        name: &str,
        tag: &str,
        prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        self.container_image_depot
            .get_container_image(name, tag, prog)
    }

    fn add_job(&mut self, spec: JobSpec, handler: JobResponseHandler) {
        // We will only get an error if the dispatcher has closed its receiver, which will only
        // happen if it ran into an error. We'll get that error when we wait in
        // `wait_for_oustanding_job`.
        let _ = self
            .dispatcher_sender
            .send(DispatcherMessage::AddJob(spec, handler));
    }

    fn stop_accepting(&mut self) -> Result<()> {
        self.dispatcher_sender.send(DispatcherMessage::Stop)?;
        Ok(())
    }

    fn wait_for_outstanding_jobs(&mut self) -> Result<()> {
        self.stop_accepting().ok();
        self.driver.stop()?;
        Ok(())
    }

    fn get_job_state_counts(&mut self) -> Result<Receiver<JobStateCounts>> {
        let (sender, recv) = mpsc::sync_channel(1);
        self.dispatcher_sender
            .send(DispatcherMessage::GetJobStateCounts(sender))?;
        Ok(recv)
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    fn process_broker_msg_single_threaded(&self, count: usize) {
        self.driver.process_broker_msg_single_threaded(count)
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    fn process_client_messages_single_threaded(&self) {
        self.driver.process_client_messages_single_threaded()
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    fn process_artifact_single_threaded(&self) {
        self.driver.process_artifact_single_threaded()
    }
}

pub fn main() -> Result<()> {
    let listener = maelstrom_client_base::listen(std::process::id())?;
    println!("started");
    rpc::run_process_client(listener.accept()?.0)
}
