use assert_matches::assert_matches;
use maelstrom_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    stats::JobStateCounts,
    JobSpec, JobStringResult,
};
use maelstrom_util::config::BrokerAddr;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv6Addr, SocketAddrV6, TcpListener, TcpStream};

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

#[derive(Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
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
        let mut buf = vec![0; u32::from_le_bytes(msg_len) as usize];
        self.stream.read_exact(&mut buf).unwrap();
        Ok(bincode::deserialize_from(&buf[..]).unwrap())
    }
}

pub struct FakeBroker {
    #[allow(dead_code)]
    listener: TcpListener,
    state: BrokerState,
    address: BrokerAddr,
}

pub struct FakeBrokerConnection {
    messages: MessageStream,
    state: BrokerState,
}

impl FakeBroker {
    pub fn new(state: BrokerState) -> Self {
        let listener =
            TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
        let address = BrokerAddr::new(listener.local_addr().unwrap());

        Self {
            listener,
            state,
            address,
        }
    }

    pub fn address(&self) -> &BrokerAddr {
        &self.address
    }

    pub fn accept(&mut self) -> FakeBrokerConnection {
        let (stream, _) = self.listener.accept().unwrap();
        let mut messages = MessageStream { stream };

        let msg: Hello = messages.next().unwrap();
        assert_matches!(msg, Hello::Client);

        FakeBrokerConnection {
            messages,
            state: self.state.clone(),
        }
    }
}

fn send_message(mut stream: &TcpStream, msg: &impl Serialize) {
    let buf = bincode::serialize(msg).unwrap();
    stream.write_all(&(buf.len() as u32).to_le_bytes()).unwrap();
    stream.write_all(&buf[..]).unwrap();
}

impl FakeBrokerConnection {
    pub fn process(&mut self, count: usize) {
        for _ in 0..count {
            let msg = self.messages.next::<ClientToBroker>().unwrap();
            match msg {
                ClientToBroker::JobRequest(id, spec) => {
                    let job_spec_matcher = job_spec_matcher(&spec);
                    match self.state.job_responses.remove(&job_spec_matcher).unwrap() {
                        JobAction::Respond(res) => send_message(
                            &self.messages.stream,
                            &BrokerToClient::JobResponse(id, res),
                        ),
                        JobAction::Ignore => (),
                    }
                }
                ClientToBroker::JobStateCountsRequest => send_message(
                    &self.messages.stream,
                    &BrokerToClient::JobStateCountsResponse(self.state.job_states),
                ),

                _ => (),
            }
        }
    }
}

#[derive(Clone)]
pub enum JobAction {
    Ignore,
    Respond(JobStringResult),
}

#[derive(Default, Clone)]
pub struct BrokerState {
    pub job_responses: HashMap<JobSpecMatcher, JobAction>,
    pub job_states: JobStateCounts,
}
