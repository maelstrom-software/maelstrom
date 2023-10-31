use anyhow::{anyhow, Result};
use meticulous_base::{
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    stats::JobStateCounts,
    ClientJobId, JobDetails, JobResult, Sha256Digest,
};
use meticulous_util::{ext::OptionExt as _, io::FixedSizeReader, net};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File},
    io::{self, Read},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::mpsc::{self, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

fn artifact_pusher_main(
    broker_addr: SocketAddr,
    path: PathBuf,
    digest: Sha256Digest,
) -> Result<()> {
    let mut stream = TcpStream::connect(broker_addr)?;
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut file = FixedSizeReader::new(file, size);
    net::write_message_to_socket(&mut stream, Hello::ArtifactPusher)?;
    net::write_message_to_socket(&mut stream, ArtifactPusherToBroker(digest, size))?;
    let copied = io::copy(&mut file, &mut stream)?;
    assert_eq!(copied, size);
    let BrokerToArtifactPusher(resp) = net::read_message_from_socket(&mut stream)?;
    resp.map_err(|e| anyhow!("Error from broker: {e}"))
}

fn add_artifact(
    artifacts: &mut HashMap<Sha256Digest, PathBuf>,
    path: PathBuf,
) -> Result<Sha256Digest> {
    let mut hasher = Sha256::new();
    match path.extension() {
        Some(ext) if ext == "tar" => {}
        _ => {
            return Err(anyhow!(
                "path \"{}\" does not end in \".tar\"",
                path.to_string_lossy()
            ));
        }
    }
    let mut f = File::open(&path)?;
    let mut buf = [0u8; 8192];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = Sha256Digest::new(hasher.finalize().into());
    artifacts.insert(digest.clone(), path);
    Ok(digest)
}

enum DispatcherMessage {
    BrokerToClient(BrokerToClient),
    AddArtifact(PathBuf, SyncSender<Result<Sha256Digest>>),
    AddJob(JobDetails, JobResponseHandler),
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
            DispatcherMessage::AddArtifact(path, sender) => {
                sender.send(add_artifact(&mut artifacts, path))?
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

pub type JobResponseHandler = Box<dyn FnOnce(ClientJobId, JobResult) -> Result<()> + Send + Sync>;

pub struct Client {
    dispatcher_sender: SyncSender<DispatcherMessage>,
    dispatcher_handle: JoinHandle<Result<()>>,
    paths: HashMap<PathBuf, Sha256Digest>,
}

impl Client {
    pub fn new(broker_addr: SocketAddr) -> Result<Self> {
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
            paths: HashMap::default(),
        })
    }

    pub fn add_artifact(&mut self, path: PathBuf) -> Result<Sha256Digest> {
        let path = fs::canonicalize(path)?;
        if let Some(digest) = self.paths.get(&path) {
            return Ok(digest.clone());
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        self.dispatcher_sender
            .send(DispatcherMessage::AddArtifact(path.clone(), sender))?;
        let digest = receiver.recv()??;
        self.paths.insert(path, digest.clone()).assert_is_none();
        Ok(digest)
    }

    pub fn add_job(&mut self, details: JobDetails, handler: JobResponseHandler) {
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
