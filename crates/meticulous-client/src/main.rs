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
    io::{self, BufRead, Read, Write as _},
    net::{SocketAddr, TcpStream, ToSocketAddrs as _},
    path::PathBuf,
    process::ExitCode,
    sync::{
        mpsc::{self, Receiver, SyncSender},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
};

struct Client {
    sender_sender: SyncSender<JobDetails>,
    sender_handle: JoinHandle<Result<u32>>,
    shared: Arc<ClientShared>,
}

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

struct ClientShared {
    lock: Mutex<ClientLocked>,
    no_outstanding_jobs: Condvar,
    broker_addr: SocketAddr,
}

#[derive(Default)]
struct ClientLocked {
    jobs: HashMap<ClientJobId, JobResult>,
    artifacts: HashMap<Sha256Digest, PathBuf>,
}

impl Client {
    fn new(broker_addr: SocketAddr) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr)?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let shared = ClientShared {
            lock: Mutex::default(),
            no_outstanding_jobs: Condvar::default(),
            broker_addr,
        };

        let sender_stream = stream.try_clone()?;
        let receiver_stream = stream;

        let (sender_sender, sender_receiver) = mpsc::sync_channel(1000);

        let sender = ClientSender {
            receiver: sender_receiver,
            stream: sender_stream,
            next_client_job_id: 0,
        };
        let sender_handle = thread::spawn(|| sender.main());

        let client = Client {
            sender_sender,
            sender_handle,
            shared: Arc::new(shared),
        };

        let shared = client.shared.clone();
        thread::spawn(|| Self::receiver_main(shared, receiver_stream));

        Ok(client)
    }

    fn receiver_main(shared: Arc<ClientShared>, mut stream: TcpStream) -> Result<()> {
        loop {
            match net::read_message_from_socket::<BrokerToClient>(&mut stream)? {
                BrokerToClient::JobResponse(cjid, result) => {
                    let mut locked = shared.lock.lock().unwrap();
                    locked.jobs.insert(cjid, result).assert_is_none();
                    shared.no_outstanding_jobs.notify_all();
                }
                BrokerToClient::TransferArtifact(digest) => {
                    let broker_addr = shared.broker_addr;
                    let path = shared
                        .lock
                        .lock()
                        .unwrap()
                        .artifacts
                        .get(&digest)
                        .unwrap()
                        .clone();
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
        self.shared
            .lock
            .lock()
            .unwrap()
            .artifacts
            .insert(digest.clone(), path)
            .assert_is_none();
        Ok(digest)
    }

    fn add_job(&mut self, details: JobDetails) -> Result<()> {
        self.sender_sender.send(details)?;
        Ok(())
    }

    fn wait_for_oustanding_jobs(self) -> Result<HashMap<ClientJobId, JobResult>> {
        drop(self.sender_sender);
        let num_jobs = self.sender_handle.join().unwrap()?;
        let guard = self.shared.lock.lock().unwrap();
        let mut guard = self
            .shared
            .no_outstanding_jobs
            .wait_while(guard, |locked| locked.jobs.len() != num_jobs as usize)
            .unwrap();
        Ok(std::mem::take(&mut guard.jobs))
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

trait BufReadExt: BufRead {
    fn has_data_left_2(&mut self) -> io::Result<bool> {
        self.fill_buf().map(|b| !b.is_empty())
    }
}

impl<T: BufRead> BufReadExt for T {}

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
    let jobs = client.wait_for_oustanding_jobs()?;
    let mut exit_code = ExitCode::from(0);
    for (cjid, result) in jobs {
        match result {
            JobResult::Ran {
                status,
                stdout,
                stderr,
            } => {
                match stdout {
                    JobOutputResult::None => {}
                    JobOutputResult::Inline(bytes) => {
                        io::stdout().write_all(&bytes)?;
                    }
                    JobOutputResult::Truncated { first, truncated } => {
                        io::stdout().write_all(&first)?;
                        io::stdout().flush()?;
                        eprintln!("job {cjid}: stdout truncated, {truncated} bytes lost");
                    }
                }
                match stderr {
                    JobOutputResult::None => {}
                    JobOutputResult::Inline(bytes) => {
                        io::stderr().write_all(&bytes)?;
                    }
                    JobOutputResult::Truncated { first, truncated } => {
                        io::stderr().write_all(&first)?;
                        eprintln!("job {cjid}: stderr truncated, {truncated} bytes lost");
                    }
                }
                match status {
                    JobStatus::Exited(0) => {}
                    JobStatus::Exited(code) => {
                        io::stdout().flush()?;
                        eprintln!("job {cjid}: exited with code {code}");
                        exit_code = ExitCode::from(code)
                    }
                    JobStatus::Signalled(signum) => {
                        io::stdout().flush()?;
                        eprintln!("job {cjid}: killed by signal {signum}");
                        exit_code = ExitCode::from(1)
                    }
                }
            }
            JobResult::ExecutionError(err) => eprintln!("job {cjid}: execution error: {err}"),
            JobResult::SystemError(err) => eprintln!("job {cjid}: system error: {err}"),
        }
    }
    Ok(exit_code)
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
