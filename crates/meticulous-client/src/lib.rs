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
use meticulous_util::{ext::OptionExt as _, fs::Fs, io::FixedSizeReader, net};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, DisplayFromStr};
use sha2::{Digest as _, Sha256};
use std::io::SeekFrom;
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Read as _, Seek as _, Write as _},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

fn artifact_pusher_main(
    broker_addr: SocketAddr,
    path: PathBuf,
    digest: Sha256Digest,
) -> Result<()> {
    let fs = Fs::new();
    let mut stream = TcpStream::connect(broker_addr)?;
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

fn dispatcher_main(
    receiver: Receiver<DispatcherMessage>,
    mut stream: TcpStream,
    broker_addr: SocketAddr,
) -> Result<()> {
    let mut stop_when_all_completed = false;
    let mut next_client_job_id = 0u32;
    let mut artifacts = HashMap::<Sha256Digest, PathBuf>::default();
    let mut handlers = HashMap::<ClientJobId, JobResponseHandler>::default();
    let mut stats_reqs: VecDeque<SyncSender<JobStateCounts>> = VecDeque::new();
    loop {
        let msg = receiver.recv()?;
        match msg {
            DispatcherMessage::BrokerToClient(BrokerToClient::JobResponse(cjid, result)) => {
                handlers.remove(&cjid).unwrap()(cjid, result)?;
                if stop_when_all_completed && handlers.is_empty() {
                    return Ok(());
                }
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::TransferArtifact(digest)) => {
                let path = artifacts.get(&digest).unwrap().clone();
                thread::spawn(move || artifact_pusher_main(broker_addr, path, digest));
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests")
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::JobStateCountsResponse(res)) => {
                stats_reqs.pop_front().unwrap().send(res).ok();
            }
            DispatcherMessage::AddArtifact(path, digest) => {
                artifacts.insert(digest, path);
            }
            DispatcherMessage::AddJob(details, handler) => {
                let cjid = next_client_job_id.into();
                handlers.insert(cjid, handler).assert_is_none();
                next_client_job_id = next_client_job_id.checked_add(1).unwrap();
                net::write_message_to_socket(
                    &mut stream,
                    ClientToBroker::JobRequest(cjid, details),
                )?;
            }
            DispatcherMessage::Stop => {
                if handlers.is_empty() {
                    return Ok(());
                }
                stop_when_all_completed = true;
            }
            DispatcherMessage::GetJobStateCounts(sender) => {
                net::write_message_to_socket(&mut stream, ClientToBroker::JobStateCountsRequest)?;
                stats_reqs.push_back(sender);
            }
        }
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

pub type JobResponseHandler = Box<dyn FnOnce(ClientJobId, JobResult) -> Result<()> + Send + Sync>;

pub struct Client {
    dispatcher_sender: SyncSender<DispatcherMessage>,
    dispatcher_handle: JoinHandle<Result<()>>,
    digest_repo: DigestRespository,
    container_image_depot: ContainerImageDepot,
    digest_to_container_env: HashMap<NonEmpty<Sha256Digest>, Vec<String>>,
    processed_artifact_paths: HashSet<PathBuf>,
}

impl Client {
    pub fn new(
        broker_addr: SocketAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr)?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let (dispatcher_sender, dispatcher_receiver) = mpsc::sync_channel(1000);

        let stream_clone = stream.try_clone()?;
        let dispatcher_handle =
            thread::spawn(move || dispatcher_main(dispatcher_receiver, stream_clone, broker_addr));

        let dispatcher_sender_clone = dispatcher_sender.clone();
        thread::spawn(move || {
            net::socket_reader(
                stream,
                dispatcher_sender_clone,
                DispatcherMessage::BrokerToClient,
            )
        });

        Ok(Client {
            dispatcher_sender,
            dispatcher_handle,
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

    fn maybe_add_container_environment(&mut self, details: &mut JobSpec) {
        for (digests, env) in &self.digest_to_container_env {
            let container_digests: HashSet<_> = digests.iter().collect();
            let job_digests: HashSet<_> = details.layers.iter().collect();
            if job_digests.is_superset(&container_digests) {
                details.environment.extend(env.iter().cloned());
            }
        }
    }

    pub fn add_job(&mut self, mut details: JobSpec, handler: JobResponseHandler) {
        self.maybe_add_container_environment(&mut details);

        // We will only get an error if the dispatcher has closed its receiver, which will only
        // happen if it ran into an error. We'll get that error when we wait in
        // `wait_for_oustanding_job`.
        let _ = self
            .dispatcher_sender
            .send(DispatcherMessage::AddJob(details, handler));
    }

    pub fn wait_for_outstanding_jobs(self) -> Result<()> {
        self.dispatcher_sender.send(DispatcherMessage::Stop)?;
        self.dispatcher_handle.join().unwrap()?;
        Ok(())
    }

    pub fn get_job_state_counts(&mut self) -> Result<JobStateCounts> {
        let (sender, recv) = mpsc::sync_channel(1);
        self.dispatcher_sender
            .send(DispatcherMessage::GetJobStateCounts(sender))?;
        Ok(recv.recv()?)
    }
}
