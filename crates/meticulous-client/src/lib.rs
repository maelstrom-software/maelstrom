use anyhow::{anyhow, Result};
use meticulous_base::{
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    ClientJobId, JobDetails, JobOutputResult, JobResult, JobStatus, Sha256Digest,
};
use meticulous_util::{ext::OptionExt as _, io::FixedSizeReader, net};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write as _},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

struct ClientSender {
    receiver: Receiver<JobDetails>,
    stream: TcpStream,
    next_client_job_id: u32,
}

impl ClientSender {
    fn main(mut self) -> Result<u32> {
        while let Ok(details) = self.receiver.recv() {
            let cjid = self.next_client_job_id.into();
            self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();
            net::write_message_to_socket(
                &mut self.stream,
                ClientToBroker::JobRequest(cjid, details),
            )?;
        }
        Ok(self.next_client_job_id)
    }
}

enum ClientReceiverMessage {
    Result(ClientJobId, JobResult),
    SenderCompleted(u32),
}

struct ClientReceiver {
    receiver: Receiver<ClientReceiverMessage>,
}

impl ClientReceiver {
    fn main(self) -> Result<ExitCode> {
        let mut jobs_completed = 0u32;
        let mut stop_at_num_jobs: Option<u32> = None;
        let mut exit_code = ExitCode::SUCCESS;
        loop {
            let msg = self.receiver.recv()?;
            match msg {
                ClientReceiverMessage::Result(cjid, result) => {
                    match result {
                        JobResult::Ran {
                            status,
                            stdout,
                            stderr,
                        } => {
                            match stdout {
                                JobOutputResult::None => {}
                                JobOutputResult::Inline(bytes) => {
                                    io::stdout().lock().write_all(&bytes)?;
                                }
                                JobOutputResult::Truncated { first, truncated } => {
                                    io::stdout().lock().write_all(&first)?;
                                    io::stdout().lock().flush()?;
                                    eprintln!(
                                        "job {cjid}: stdout truncated, {truncated} bytes lost"
                                    );
                                }
                            }
                            match stderr {
                                JobOutputResult::None => {}
                                JobOutputResult::Inline(bytes) => {
                                    io::stderr().lock().write_all(&bytes)?;
                                }
                                JobOutputResult::Truncated { first, truncated } => {
                                    io::stderr().lock().write_all(&first)?;
                                    eprintln!(
                                        "job {cjid}: stderr truncated, {truncated} bytes lost"
                                    );
                                }
                            }
                            match status {
                                JobStatus::Exited(0) => {}
                                JobStatus::Exited(code) => {
                                    io::stdout().lock().flush()?;
                                    eprintln!("job {cjid}: exited with code {code}");
                                    exit_code = ExitCode::from(code)
                                }
                                JobStatus::Signalled(signum) => {
                                    io::stdout().lock().flush()?;
                                    eprintln!("job {cjid}: killed by signal {signum}");
                                    exit_code = ExitCode::FAILURE
                                }
                            }
                        }
                        JobResult::ExecutionError(err) => {
                            eprintln!("job {cjid}: execution error: {err}")
                        }
                        JobResult::SystemError(err) => eprintln!("job {cjid}: system error: {err}"),
                    }
                    jobs_completed = jobs_completed.checked_add(1).unwrap();
                    if stop_at_num_jobs == Some(jobs_completed) {
                        return Ok(exit_code);
                    }
                }
                ClientReceiverMessage::SenderCompleted(num_jobs) => {
                    stop_at_num_jobs = Some(num_jobs);
                    if stop_at_num_jobs == Some(jobs_completed) {
                        return Ok(exit_code);
                    }
                }
            }
        }
    }
}

struct ClientSocketReceiver {
    artifacts: Arc<Mutex<HashMap<Sha256Digest, PathBuf>>>,
    broker_addr: SocketAddr,
    receiver_sender: SyncSender<ClientReceiverMessage>,
    stream: TcpStream,
}

impl ClientSocketReceiver {
    fn main(mut self) -> Result<()> {
        loop {
            match net::read_message_from_socket::<BrokerToClient>(&mut self.stream)? {
                BrokerToClient::JobResponse(cjid, result) => {
                    self.receiver_sender
                        .send(ClientReceiverMessage::Result(cjid, result))
                        .unwrap();
                }
                BrokerToClient::TransferArtifact(digest) => {
                    let path = self.artifacts.lock().unwrap().get(&digest).unwrap().clone();
                    let artifact_pusher = ClientArtifactPusher {
                        broker_addr: self.broker_addr,
                        path,
                        digest,
                    };
                    thread::spawn(move || artifact_pusher.main());
                }
                BrokerToClient::StatisticsResponse(_) => {
                    unimplemented!("this client doesn't send statistics requests")
                }
            }
        }
    }
}

struct ClientArtifactPusher {
    broker_addr: SocketAddr,
    path: PathBuf,
    digest: Sha256Digest,
}

impl ClientArtifactPusher {
    fn main(self) -> Result<()> {
        let file = File::open(self.path)?;
        let mut stream = TcpStream::connect(self.broker_addr)?;
        let size = file.metadata()?.len();
        let mut file = FixedSizeReader::new(file, size);
        net::write_message_to_socket(&mut stream, Hello::ArtifactPusher)?;
        net::write_message_to_socket(&mut stream, ArtifactPusherToBroker(self.digest, size))?;
        let copied = io::copy(&mut file, &mut stream)?;
        assert_eq!(copied, size);
        let BrokerToArtifactPusher(resp) = net::read_message_from_socket(&mut stream)?;
        resp.map_err(|e| anyhow!("Error from broker: {e}"))
    }
}

pub struct Client {
    sender_sender: SyncSender<JobDetails>,
    sender_handle: JoinHandle<Result<u32>>,
    receiver_sender: SyncSender<ClientReceiverMessage>,
    receiver_handle: JoinHandle<Result<ExitCode>>,
    artifacts: Arc<Mutex<HashMap<Sha256Digest, PathBuf>>>,
    paths: HashMap<PathBuf, Sha256Digest>,
}

impl Client {
    pub fn new(broker_addr: SocketAddr) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr)?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let sender_stream = stream.try_clone()?;

        let (sender_sender, sender_receiver) = mpsc::sync_channel(1000);

        let sender = ClientSender {
            receiver: sender_receiver,
            stream: sender_stream,
            next_client_job_id: 0,
        };
        let sender_handle = thread::spawn(|| sender.main());

        let (receiver_sender, receiver_receiver) = mpsc::sync_channel(1000);

        let receiver = ClientReceiver {
            receiver: receiver_receiver,
        };
        let receiver_handle = thread::spawn(|| receiver.main());

        let artifacts = Arc::new(Mutex::new(HashMap::default()));

        let socket_receiver = ClientSocketReceiver {
            artifacts: artifacts.clone(),
            broker_addr,
            receiver_sender: receiver_sender.clone(),
            stream,
        };

        thread::spawn(move || socket_receiver.main());

        Ok(Client {
            sender_sender,
            sender_handle,
            receiver_sender,
            receiver_handle,
            artifacts,
            paths: HashMap::default(),
        })
    }

    pub fn add_artifact(&mut self, path: &Path) -> Result<Sha256Digest> {
        let path = fs::canonicalize(path)?;
        if let Some(digest) = self.paths.get(&path) {
            return Ok(digest.clone());
        }
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
        self.paths
            .insert(path.clone(), digest.clone())
            .assert_is_none();
        self.artifacts.lock().unwrap().insert(digest.clone(), path);
        Ok(digest)
    }

    pub fn add_job(&mut self, details: JobDetails) {
        // We will only get an error if the sender has closed its receiver, which will only happen
        // if it had an error writing to the socket. We'll get that error when we wait in
        // `wait_for_oustanding_job`.
        let _ = self.sender_sender.send(details);
    }

    pub fn wait_for_oustanding_jobs(self) -> Result<ExitCode> {
        drop(self.sender_sender);
        let num_jobs = self.sender_handle.join().unwrap()?;
        self.receiver_sender
            .send(ClientReceiverMessage::SenderCompleted(num_jobs))
            .unwrap();
        self.receiver_handle.join().unwrap()
    }
}
