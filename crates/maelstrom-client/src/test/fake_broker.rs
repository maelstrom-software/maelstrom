use assert_matches::assert_matches;
use maelstrom_base::{
    proto::{
        self, ArtifactPusherToBroker, BrokerToArtifactPusher, BrokerToClient, ClientToBroker, Hello,
    },
    stats::JobStateCounts,
    ClientJobId, JobOutcomeResult, JobSpec,
};
use maelstrom_util::{config::BrokerAddr, ext::OptionExt as _, fs::Fs, io::FixedSizeReader};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv6Addr, SocketAddrV6, TcpListener, TcpStream};
use std::path::Path;

fn job_spec_matcher(spec: &JobSpec) -> JobSpecMatcher {
    let binary = spec
        .program
        .as_str()
        .split('/')
        .last()
        .unwrap()
        .split('-')
        .next()
        .unwrap()
        .into();
    let test_name = spec
        .arguments
        .iter()
        .find(|a| !a.starts_with('-'))
        .unwrap()
        .clone();
    JobSpecMatcher {
        binary,
        first_arg: test_name,
    }
}

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct JobSpecMatcher {
    pub binary: String,
    pub first_arg: String,
}

struct MessageStream {
    stream: TcpStream,
}

impl MessageStream {
    fn next<T: DeserializeOwned>(&mut self) -> io::Result<T> {
        let mut msg_len: [u8; 4] = [0; 4];
        self.stream.read_exact(&mut msg_len)?;
        let mut buf = vec![0; u32::from_be_bytes(msg_len) as usize];
        self.stream.read_exact(&mut buf).unwrap();
        Ok(proto::deserialize_from(&buf[..]).unwrap())
    }

    fn into_inner(self) -> TcpStream {
        self.stream
    }
}

pub struct FakeBroker {
    #[allow(dead_code)]
    listener: TcpListener,
    state: FakeBrokerState,
    address: BrokerAddr,
    log: slog::Logger,
}

pub struct FakeBrokerConnection {
    messages: MessageStream,
    state: FakeBrokerState,
    pending_response: Option<(ClientJobId, FakeBrokerJobAction)>,
    log: slog::Logger,
}

impl FakeBroker {
    pub fn new(state: FakeBrokerState, log: slog::Logger) -> Self {
        let listener =
            TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
        let address = BrokerAddr::new(listener.local_addr().unwrap());

        Self {
            listener,
            state,
            address,
            log,
        }
    }

    pub fn address(&self) -> &BrokerAddr {
        &self.address
    }

    pub fn accept(&mut self) -> FakeBrokerConnection {
        slog::debug!(self.log, "FakeBroker: waiting for connection");
        let (stream, _) = self.listener.accept().unwrap();
        let mut messages = MessageStream { stream };
        slog::debug!(self.log, "FakeBroker: accepted connection");

        let msg: Hello = messages.next().unwrap();
        assert_matches!(msg, Hello::Client);

        FakeBrokerConnection {
            messages,
            state: self.state.clone(),
            pending_response: None,
            log: self.log.clone(),
        }
    }

    pub fn receive_artifact(&mut self, digest_dir: &Path) {
        slog::debug!(self.log, "FakeBroker: waiting for artifact connection");
        let (stream, _) = self.listener.accept().unwrap();
        let mut messages = MessageStream { stream };
        slog::debug!(self.log, "FakeBroker: accepted artifact connection");

        let msg: Hello = messages.next().unwrap();
        assert_matches!(msg, Hello::ArtifactPusher);

        let ArtifactPusherToBroker(digest, size) = messages.next().unwrap();
        let destination = digest_dir.join(digest.to_string());

        let fs = Fs::new();
        let mut stream = messages.into_inner();
        let mut file = fs.create_file(&destination).unwrap();
        io::copy(&mut FixedSizeReader::new(&mut stream, size), &mut file).unwrap();
        send_message(&stream, &BrokerToArtifactPusher(Ok(())));
        slog::debug!(self.log, "FakeBroker: got artifact"; "path" => ?destination);
    }
}

fn send_message(mut stream: &TcpStream, msg: &impl Serialize) {
    let buf = proto::serialize(msg).unwrap();
    stream.write_all(&(buf.len() as u32).to_be_bytes()).unwrap();
    stream.write_all(&buf[..]).unwrap();
}

impl FakeBrokerConnection {
    fn fetch_layers(&self, spec: &JobSpec) {
        for (digest, _type) in &spec.layers {
            slog::debug!(self.log, "FakeBroker: asking for layer"; "digest" => ?digest);
            send_message(
                &self.messages.stream,
                &BrokerToClient::TransferArtifact(digest.clone()),
            );
        }
    }

    pub fn process(&mut self, count: usize, fetch_layers: bool) {
        slog::debug!(
            self.log, "FakeBroker: proccess";
            "count" => count,
            "fetch_layers" => fetch_layers
        );
        for _ in 0..count {
            if let Some((id, response)) = self.pending_response.take() {
                match response {
                    FakeBrokerJobAction::Respond(res) => {
                        send_message(&self.messages.stream, &BrokerToClient::JobResponse(id, res))
                    }
                    FakeBrokerJobAction::Ignore => (),
                }
                continue;
            }

            let msg = self.messages.next::<ClientToBroker>().unwrap();
            match msg {
                ClientToBroker::JobRequest(id, spec) => {
                    slog::debug!(self.log, "FakeBroker: got JobRequest");
                    let job_spec_matcher = job_spec_matcher(&spec);
                    let response = self.state.job_responses.remove(&job_spec_matcher).unwrap();

                    if fetch_layers {
                        self.fetch_layers(&spec);
                        self.pending_response
                            .replace((id, response))
                            .assert_is_none();
                        continue;
                    }

                    match response {
                        FakeBrokerJobAction::Respond(res) => send_message(
                            &self.messages.stream,
                            &BrokerToClient::JobResponse(id, res),
                        ),
                        FakeBrokerJobAction::Ignore => (),
                    }
                }
                ClientToBroker::JobStateCountsRequest => {
                    slog::debug!(self.log, "FakeBroker: got JobStateCountsRequest");
                    send_message(
                        &self.messages.stream,
                        &BrokerToClient::JobStateCountsResponse(self.state.job_states),
                    );
                }

                _ => (),
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum FakeBrokerJobAction {
    Ignore,
    Respond(JobOutcomeResult),
}

#[derive(Default, Debug, Clone)]
pub struct FakeBrokerState {
    pub job_responses: HashMap<JobSpecMatcher, FakeBrokerJobAction>,
    pub job_states: JobStateCounts,
}
