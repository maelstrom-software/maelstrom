use anyhow::{anyhow, Result};
use clap::Parser;
use meticulous_base::{
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    ClientJobId, JobDetails, JobOutputResult, JobResult, JobStatus, Sha256Digest,
};
use meticulous_util::{ext::OptionExt as _, io::FixedSizeReader, net};
use serde::Deserialize;
use serde_json::{self, Deserializer};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write as _},
    net::{SocketAddr, TcpStream, ToSocketAddrs as _},
    path::PathBuf,
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
        let mut exit_code = ExitCode::from(0);
        let mut stdout_locked = io::stdout().lock();
        let mut stderr_locked = io::stderr().lock();
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
                                    stdout_locked.write_all(&bytes)?;
                                }
                                JobOutputResult::Truncated { first, truncated } => {
                                    stdout_locked.write_all(&first)?;
                                    stdout_locked.flush()?;
                                    eprintln!(
                                        "job {cjid}: stdout truncated, {truncated} bytes lost"
                                    );
                                }
                            }
                            match stderr {
                                JobOutputResult::None => {}
                                JobOutputResult::Inline(bytes) => {
                                    stderr_locked.write_all(&bytes)?;
                                }
                                JobOutputResult::Truncated { first, truncated } => {
                                    stderr_locked.write_all(&first)?;
                                    eprintln!(
                                        "job {cjid}: stderr truncated, {truncated} bytes lost"
                                    );
                                }
                            }
                            match status {
                                JobStatus::Exited(0) => {}
                                JobStatus::Exited(code) => {
                                    stdout_locked.flush()?;
                                    eprintln!("job {cjid}: exited with code {code}");
                                    exit_code = ExitCode::from(code)
                                }
                                JobStatus::Signalled(signum) => {
                                    stdout_locked.flush()?;
                                    eprintln!("job {cjid}: killed by signal {signum}");
                                    exit_code = ExitCode::from(1)
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

struct Client {
    sender_sender: SyncSender<JobDetails>,
    sender_handle: JoinHandle<Result<u32>>,
    receiver_sender: SyncSender<ClientReceiverMessage>,
    receiver_handle: JoinHandle<Result<ExitCode>>,
    artifacts: Arc<Mutex<HashMap<Sha256Digest, PathBuf>>>,
}

impl Client {
    fn new(broker_addr: SocketAddr) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr)?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let sender_stream = stream.try_clone()?;
        let receiver_stream = stream;

        let (sender_sender, sender_receiver) = mpsc::sync_channel(1000);

        let sender = ClientSender {
            receiver: sender_receiver,
            stream: sender_stream,
            next_client_job_id: 0,
        };
        let sender_handle = thread::spawn(|| sender.main());

        let (receiver_sender, receiver_receiver) = mpsc::sync_channel(1000);
        let receiver_sender_clone = receiver_sender.clone();

        let receiver = ClientReceiver {
            receiver: receiver_receiver,
        };
        let receiver_handle = thread::spawn(|| receiver.main());

        let artifacts = Arc::new(Mutex::new(HashMap::default()));
        let artifacts_clone = artifacts.clone();

        let client = Client {
            sender_sender,
            sender_handle,
            receiver_sender,
            receiver_handle,
            artifacts,
        };

        thread::spawn(move || {
            Self::receiver_main(
                artifacts_clone,
                broker_addr,
                receiver_sender_clone,
                receiver_stream,
            )
        });

        Ok(client)
    }

    fn receiver_main(
        artifacts: Arc<Mutex<HashMap<Sha256Digest, PathBuf>>>,
        broker_addr: SocketAddr,
        receiver_sender: SyncSender<ClientReceiverMessage>,
        mut stream: TcpStream,
    ) -> Result<()> {
        loop {
            match net::read_message_from_socket::<BrokerToClient>(&mut stream)? {
                BrokerToClient::JobResponse(cjid, result) => {
                    receiver_sender
                        .send(ClientReceiverMessage::Result(cjid, result))
                        .unwrap();
                }
                BrokerToClient::TransferArtifact(digest) => {
                    let path = artifacts.lock().unwrap().get(&digest).unwrap().clone();
                    thread::spawn(move || Self::transfer_artifact_main(broker_addr, digest, path));
                }
                BrokerToClient::StatisticsResponse(_) => {
                    unimplemented!("this client doesn't send statistics requests")
                }
            }
        }
    }

    fn transfer_artifact_main(
        broker_addr: SocketAddr,
        digest: Sha256Digest,
        path: PathBuf,
    ) -> Result<()> {
        let file = File::open(path)?;
        let mut stream = TcpStream::connect(broker_addr)?;
        let size = file.metadata()?.len();
        let mut file = FixedSizeReader::new(file, size);
        net::write_message_to_socket(&mut stream, Hello::ArtifactPusher)?;
        net::write_message_to_socket(&mut stream, ArtifactPusherToBroker(digest, size))?;
        let copied = io::copy(&mut file, &mut stream)?;
        assert_eq!(copied, size);
        let BrokerToArtifactPusher(resp) = net::read_message_from_socket(&mut stream)?;
        resp.map_err(|e| anyhow!("Error from broker: {e}"))
    }

    #[allow(dead_code)]
    fn add_artifact(&mut self, path: PathBuf) -> Result<Sha256Digest> {
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
        self.artifacts
            .lock()
            .unwrap()
            .insert(digest.clone(), path)
            .assert_is_none();
        Ok(digest)
    }

    fn add_job(&mut self, details: JobDetails) -> Result<()> {
        self.sender_sender.send(details)?;
        Ok(())
    }

    fn wait_for_oustanding_jobs(self) -> Result<ExitCode> {
        drop(self.sender_sender);
        let num_jobs = self.sender_handle.join().unwrap()?;
        self.receiver_sender
            .send(ClientReceiverMessage::SenderCompleted(num_jobs))
            .unwrap();
        self.receiver_handle.join().unwrap()
    }
}

/// The meticulous client. This process sends jobs to the broker to be executed.
#[derive(Parser)]
#[command(version)]
struct CliOptions {
    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000"
    #[arg(short = 'b', long)]
    broker: String,

    /// File to read jobs from instead of stdin.
    #[arg(short = 'f', long)]
    file: Option<PathBuf>,
}

fn parse_socket_addr(value: String) -> Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = value.to_socket_addrs()?.collect();
    // It's not clear how we could end up with an empty iterator. We'll assume that's
    // impossible until proven wrong.
    Ok(*addrs.get(0).unwrap())
}

#[derive(Deserialize, Debug)]
struct JobDescription {
    program: String,
    arguments: Option<Vec<String>>,
    layers: Option<Vec<String>>,
}

fn main() -> Result<ExitCode> {
    let cli_options = CliOptions::parse();
    let mut client = Client::new(parse_socket_addr(cli_options.broker)?)?;
    let layer_map: HashMap<String, Sha256Digest> = HashMap::default();
    //    for (name, path) in config.layers {
    //        layer_map.insert(name, client.add_artifact(path.into())?);
    //    }
    let reader: Box<dyn Read> = if let Some(file) = cli_options.file {
        Box::new(File::open(file)?)
    } else {
        Box::new(io::stdin().lock())
    };
    let jobs = Deserializer::from_reader(reader).into_iter::<JobDescription>();
    for job in jobs {
        let job = job?;
        client.add_job(JobDetails {
            program: job.program,
            arguments: if let Some(args) = job.arguments {
                args
            } else {
                vec![]
            },
            layers: if let Some(job_layers) = job.layers {
                let mut layers = vec![];
                for layer in job_layers {
                    layers.push(layer_map.get(&layer).unwrap().clone());
                }
                layers
            } else {
                vec![]
            },
        })?;
    }
    client.wait_for_oustanding_jobs()
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
