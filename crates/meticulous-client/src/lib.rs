use anyhow::{anyhow, Result};
use meticulous_base::{
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    JobDetails, JobOutputResult, JobResult, JobStatus, Sha256Digest,
};
use meticulous_util::{ext::OptionExt as _, io::FixedSizeReader, net};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write as _},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    process::ExitCode,
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
    AddJob(JobDetails),
    Stop,
}

fn dispatcher_main(
    receiver: Receiver<DispatcherMessage>,
    mut stream: TcpStream,
    broker_addr: SocketAddr,
) -> Result<ExitCode> {
    let mut jobs_completed = 0u32;
    let mut stop_at_num_jobs: Option<u32> = None;
    let mut exit_code = ExitCode::SUCCESS;
    let mut next_client_job_id = 0u32;
    let mut artifacts = HashMap::<Sha256Digest, PathBuf>::default();
    loop {
        let msg = receiver.recv()?;
        match msg {
            DispatcherMessage::BrokerToClient(BrokerToClient::JobResponse(cjid, result)) => {
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
                                eprintln!("job {cjid}: stdout truncated, {truncated} bytes lost");
                            }
                        }
                        match stderr {
                            JobOutputResult::None => {}
                            JobOutputResult::Inline(bytes) => {
                                io::stderr().lock().write_all(&bytes)?;
                            }
                            JobOutputResult::Truncated { first, truncated } => {
                                io::stderr().lock().write_all(&first)?;
                                eprintln!("job {cjid}: stderr truncated, {truncated} bytes lost");
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
            DispatcherMessage::BrokerToClient(BrokerToClient::TransferArtifact(digest)) => {
                let path = artifacts.get(&digest).unwrap().clone();
                thread::spawn(move || artifact_pusher_main(broker_addr, path, digest));
            }
            DispatcherMessage::BrokerToClient(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests")
            }
            DispatcherMessage::AddArtifact(path, sender) => {
                sender.send(add_artifact(&mut artifacts, path))?
            }
            DispatcherMessage::AddJob(details) => {
                let cjid = next_client_job_id.into();
                next_client_job_id = next_client_job_id.checked_add(1).unwrap();
                net::write_message_to_socket(
                    &mut stream,
                    ClientToBroker::JobRequest(cjid, details),
                )?;
            }
            DispatcherMessage::Stop => {
                stop_at_num_jobs = Some(next_client_job_id);
                if stop_at_num_jobs == Some(jobs_completed) {
                    return Ok(exit_code);
                }
            }
        }
    }
}

pub struct Client {
    dispatcher_sender: SyncSender<DispatcherMessage>,
    dispatcher_handle: JoinHandle<Result<ExitCode>>,
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

    pub fn add_job(&mut self, details: JobDetails) {
        // We will only get an error if the sender has closed its receiver, which will only happen
        // if it had an error writing to the socket. We'll get that error when we wait in
        // `wait_for_oustanding_job`.
        let _ = self
            .dispatcher_sender
            .send(DispatcherMessage::AddJob(details));
    }

    pub fn wait_for_oustanding_jobs(self) -> Result<ExitCode> {
        self.dispatcher_sender.send(DispatcherMessage::Stop)?;
        self.dispatcher_handle.join().unwrap()
    }
}
