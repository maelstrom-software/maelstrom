use crate::{comm, Client as ProcessClient};
use anyhow::{bail, Result};
use maelstrom_base::proto;
use maelstrom_container::ProgressTracker;

use std::os::linux::net::SocketAddrExt as _;
use std::os::unix::net::{SocketAddr, UnixListener, UnixStream};
use std::sync::{Arc, Mutex};
use std::thread;

fn address_from_pid(id: u32) -> Result<SocketAddr> {
    let mut name = b"maelstrom-client".to_vec();
    name.extend(id.to_be_bytes());
    Ok(SocketAddr::from_abstract_name(name)?)
}

pub fn listen(id: u32) -> Result<UnixListener> {
    Ok(UnixListener::bind_addr(&address_from_pid(id)?)?)
}

pub fn connect(id: u32) -> Result<UnixStream> {
    let mut name = b"maelstrom-client".to_vec();
    name.extend(id.to_be_bytes());
    Ok(UnixStream::connect_addr(&address_from_pid(id)?)?)
}

struct ProcessClientSender {
    sock: Mutex<UnixStream>,
}

impl ProcessClientSender {
    fn new(sock: UnixStream) -> Self {
        Self {
            sock: Mutex::new(sock),
        }
    }

    fn send(&self, id: comm::MessageId, msg: comm::Response) -> Result<()> {
        self.send_inner(id, true /* finished */, msg)
    }

    fn send_in_progress(&self, id: comm::MessageId, msg: comm::Response) -> Result<()> {
        self.send_inner(id, false /* finished */, msg)
    }

    fn send_inner(&self, id: comm::MessageId, finished: bool, msg: comm::Response) -> Result<()> {
        let sock = self.sock.lock().unwrap();
        proto::serialize_into(
            &*sock,
            &comm::Message {
                id,
                finished,
                body: msg,
            },
        )?;
        Ok(())
    }
}

#[derive(Clone)]
struct ProgressSender<ResponseFactoryT> {
    sender: Arc<ProcessClientSender>,
    id: comm::MessageId,
    response_factory: ResponseFactoryT,
}

impl<ResponseFactoryT> ProgressSender<ResponseFactoryT> {
    fn new(
        sender: Arc<ProcessClientSender>,
        id: comm::MessageId,
        response_factory: ResponseFactoryT,
    ) -> Self {
        Self {
            sender,
            id,
            response_factory,
        }
    }
}

impl<ResponseFactoryT> ProgressTracker for ProgressSender<ResponseFactoryT>
where
    ResponseFactoryT: Fn(comm::ProgressInfo) -> comm::Response + Send + Unpin + Clone + 'static,
{
    fn set_length(&self, length: u64) {
        let _ = self.sender.send_in_progress(
            self.id,
            (self.response_factory)(comm::ProgressInfo::Length(length)),
        );
    }

    fn inc(&self, v: u64) {
        let _ = self
            .sender
            .send_in_progress(self.id, (self.response_factory)(comm::ProgressInfo::Inc(v)));
    }
}

struct ProcessClientHandler {
    sender: Arc<ProcessClientSender>,
    client: ProcessClient,
}

impl ProcessClientHandler {
    fn new(sender: ProcessClientSender, client: ProcessClient) -> Self {
        Self {
            sender: Arc::new(sender),
            client,
        }
    }

    #[allow(clippy::unit_arg)]
    fn handle_msg<'a, 'b>(
        &mut self,
        msg: comm::Message<comm::Request>,
        scope: &'a thread::Scope<'b, '_>,
    ) -> Result<()>
    where
        'a: 'b,
    {
        let id = msg.id;
        match msg.body {
            comm::Request::Start { .. } => {
                self.sender
                    .send(id, comm::Response::Start(Err("unexpected request".into())))?;
            }
            comm::Request::AddArtifact { path } => {
                self.sender.send(
                    id,
                    comm::Response::AddArtifact(
                        self.client.add_artifact(&path).map_err(|e| e.into()),
                    ),
                )?;
            }
            comm::Request::AddLayer { layer } => {
                self.sender.send(
                    id,
                    comm::Response::AddLayer(self.client.add_layer(layer).map_err(|e| e.into())),
                )?;
            }
            comm::Request::AddJob { spec } => {
                let other_sender = self.sender.clone();
                self.client.add_job(
                    spec,
                    Box::new(move |cjid, result| {
                        let _ = other_sender.send(id, comm::Response::AddJob(Ok((cjid, result))));
                    }),
                );
            }
            comm::Request::GetContainerImage { name, tag } => {
                let prog = ProgressSender::new(self.sender.clone(), id, |v| {
                    comm::Response::GetContainerImage(Ok(comm::ProgressResponse::InProgress(v)))
                });
                self.sender.send(
                    id,
                    comm::Response::GetContainerImage(
                        self.client
                            .get_container_image(&name, &tag, prog)
                            .map_err(|e| e.into())
                            .map(comm::ProgressResponse::Done),
                    ),
                )?;
            }
            comm::Request::StopAccepting => {
                self.sender.send(
                    id,
                    comm::Response::StopAccepting(
                        self.client.stop_accepting().map_err(|e| e.into()),
                    ),
                )?;
            }
            comm::Request::WaitForOutstandingJobs => {
                self.sender.send(
                    id,
                    comm::Response::WaitForOutstandingJobs(
                        self.client
                            .wait_for_outstanding_jobs()
                            .map_err(|e| e.into()),
                    ),
                )?;
            }
            comm::Request::GetJobStateCounts => match self.client.get_job_state_counts() {
                Ok(recv) => {
                    let other_sender = self.sender.clone();
                    scope.spawn(move || {
                        other_sender.send(
                            id,
                            comm::Response::GetJobStateCounts(
                                recv.recv().map_err(|_| "unexpected error".into()),
                            ),
                        )
                    });
                }
                Err(e) => self
                    .sender
                    .send(id, comm::Response::GetJobStateCounts(Err(e.into())))?,
            },
            comm::Request::ProcessBrokerMsgSingleThreaded { count } => {
                self.sender.send(
                    id,
                    comm::Response::ProcessBrokerMsgSingleThreaded(Ok(self
                        .client
                        .process_broker_msg_single_threaded(count))),
                )?;
            }
            comm::Request::ProcessClientMessagesSingleThreaded => {
                self.sender.send(
                    id,
                    comm::Response::ProcessClientMessagesSingleThreaded(Ok(self
                        .client
                        .process_client_messages_single_threaded())),
                )?;
            }
            comm::Request::ProcessArtifactSingleThreaded => {
                self.sender.send(
                    id,
                    comm::Response::ProcessArtifactSingleThreaded(Ok(self
                        .client
                        .process_artifact_single_threaded())),
                )?;
            }
        }
        Ok(())
    }
}

pub fn run_process_client(mut sock: UnixStream) -> Result<()> {
    let req: comm::Message<comm::Request> = proto::deserialize_from(&mut sock)?;
    let comm::Message {
        id: start_message_id,
        finished: false,
        body:
            comm::Request::Start {
                driver_mode,
                broker_addr,
                project_dir,
                cache_dir,
            },
    } = req
    else {
        bail!("expected start message, got {req:?}")
    };

    let mut read_sock = sock.try_clone()?;

    let sender = ProcessClientSender::new(sock);

    let client = match ProcessClient::new(driver_mode, broker_addr, project_dir, cache_dir) {
        Ok(c) => {
            sender.send(start_message_id, comm::Response::Start(Ok(())))?;
            c
        }
        Err(e) => {
            sender.send(start_message_id, comm::Response::Start(Err(e.into())))?;
            return Ok(());
        }
    };

    let mut handler = ProcessClientHandler::new(sender, client);

    thread::scope(|scope| -> Result<()> {
        loop {
            match proto::deserialize_from::<_, comm::Message<comm::Request>>(&mut read_sock) {
                Ok(msg) => handler.handle_msg(msg, scope)?,
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
    })
}
