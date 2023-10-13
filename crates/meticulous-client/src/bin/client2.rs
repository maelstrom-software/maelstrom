use anyhow::anyhow;
use meticulous_base::{
    proto::{
        ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    ClientJobId, JobDetails, JobResult, Sha256Digest,
};
use meticulous_util::{
    error::Result,
    net::{self, FixedSizeReader},
    OptionExt,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    thread,
};

#[derive(Deserialize, Debug)]
struct JobDescription {
    program: String,
    arguments: Option<Vec<String>>,
    layers: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct Config {
    broker: String,
    tmpdir: String,
    jobs: Vec<JobDescription>,
    layers: HashMap<String, String>,
}

const FILE: &str = r#"
broker = "127.0.0.1:1234"
tmpdir = "run/client/tmp"

[[jobs]]
program = "foo"
arguments = ["arg1", "arg2"]
layers = ["web"]

[[jobs]]
program = "bar"
arguments = ["barg1", "barg2"]

[layers]
web = "target/web.tar"
"#;

struct Client {
    stream: TcpStream,
    next_client_job_id: u32,
    shared: Arc<ClientShared>,
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
    outstanding_jobs: u32,
}

impl Client {
    fn new(broker_addr: SocketAddr, _tmpdir: PathBuf) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr)?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let shared = ClientShared {
            lock: Mutex::default(),
            no_outstanding_jobs: Condvar::default(),
            broker_addr,
        };
        let client = Client {
            stream,
            next_client_job_id: 0,
            shared: Arc::new(shared),
        };

        let stream = client.stream.try_clone().unwrap();
        let shared = client.shared.clone();
        thread::spawn(|| Self::receiver_main(shared, stream));

        Ok(client)
    }

    fn receiver_main(shared: Arc<ClientShared>, mut stream: TcpStream) -> Result<()> {
        loop {
            match net::read_message_from_socket::<BrokerToClient>(&mut stream)? {
                BrokerToClient::JobResponse(cjid, result) => {
                    let mut locked = shared.lock.lock().unwrap();
                    locked.jobs.insert(cjid, result).assert_is_none();
                    locked.outstanding_jobs = locked.outstanding_jobs.checked_sub(1).unwrap();
                    if locked.outstanding_jobs == 0 {
                        shared.no_outstanding_jobs.notify_all();
                    }
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
        let digest = Sha256Digest(hasher.finalize().into());
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
        let cjid = self.next_client_job_id.into();
        self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();
        {
            let mut locked = self.shared.lock.lock().unwrap();
            locked.outstanding_jobs = locked.outstanding_jobs.checked_add(1).unwrap();
        }
        net::write_message_to_socket(&mut self.stream, ClientToBroker::JobRequest(cjid, details))?;
        Ok(())
    }

    fn wait_for_oustanding_jobs(&mut self) {
        let _ = self
            .shared
            .no_outstanding_jobs
            .wait_while(self.shared.lock.lock().unwrap(), |locked| {
                locked.outstanding_jobs != 0
            });
    }
}

fn main() -> Result<()> {
    let config: Config = toml::from_str(FILE).unwrap();

    println!("{:?}", config);

    let mut client = Client::new(config.broker.parse()?, config.tmpdir.into())?;
    let mut layer_map: HashMap<String, Sha256Digest> = HashMap::default();
    for (name, path) in config.layers {
        layer_map.insert(name, client.add_artifact(path.into())?);
    }
    for job in config.jobs {
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
    client.wait_for_oustanding_jobs();
    Ok(())
}
