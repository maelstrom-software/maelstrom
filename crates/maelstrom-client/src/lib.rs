pub use maelstrom_client_process::{
    spec, test, ClientDriverMode, JobResponseHandler, MANIFEST_DIR,
};

use anyhow::{anyhow, bail, Result};
use indicatif::ProgressBar;
use maelstrom_base::{proto, stats::JobStateCounts, ArtifactType, JobSpec, Sha256Digest};
use maelstrom_client_process::{comm, Client as ProcessClient};
use maelstrom_container::ContainerImage;
use maelstrom_util::config::BrokerAddr;
use spec::Layer;
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex,
};
use std::thread;

type ResponseCallback = Box<dyn FnOnce(comm::Response) + Send + Sync>;

fn print_error(label: &str, res: Result<()>) -> bool {
    if let Err(e) = res {
        eprintln!("{label}: error: {e:?}");
        true
    } else {
        false
    }
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
        let sock = self.sock.lock().unwrap();
        proto::serialize_into(&*sock, &comm::Message { id, body: msg })?;
        Ok(())
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
                self.sender.send(
                    id,
                    comm::Response::GetContainerImage(
                        self.client
                            .get_container_image(&name, &tag, ProgressBar::hidden())
                            .map_err(|e| e.into()),
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

fn run_process_client(mut sock: UnixStream) -> Result<()> {
    let req: comm::Message<comm::Request> = proto::deserialize_from(&mut sock)?;
    let comm::Message {
        id: start_message_id,
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

struct Dispatcher {
    sock: UnixStream,
    outstanding: HashMap<comm::MessageId, ResponseCallback>,
    next_message_id: comm::MessageId,
    drained_error: Option<comm::CommunicationError>,
}

impl Dispatcher {
    fn new(sock: UnixStream) -> Self {
        Self {
            sock,
            outstanding: HashMap::new(),
            next_message_id: comm::MessageId::default(),
            drained_error: None,
        }
    }

    fn get_message(&mut self, message: comm::Message<comm::Response>) -> Result<()> {
        let callback = self
            .outstanding
            .remove(&message.id)
            .expect("unexpected message");
        callback(message.body);
        Ok(())
    }

    fn send_message(&mut self, request: comm::Request, callback: ResponseCallback) -> Result<()> {
        if let Some(err) = &self.drained_error {
            callback(comm::Response::Error(err.clone()));
            return Ok(());
        }

        self.outstanding.insert(self.next_message_id, callback);
        proto::serialize_into(
            &mut self.sock,
            &comm::Message {
                id: self.next_message_id,
                body: request,
            },
        )?;
        self.next_message_id = self.next_message_id.next();
        Ok(())
    }

    fn drain(&mut self, error: impl Into<comm::CommunicationError>) {
        let err = error.into();
        for (_, callback) in self.outstanding.drain() {
            callback(comm::Response::Error(err.clone()))
        }
        self.drained_error = Some(err);
    }

    fn stop(&mut self) -> Result<()> {
        self.sock.shutdown(std::net::Shutdown::Both)?;
        if let Some(err) = &self.drained_error {
            return Err(err.clone().into());
        }
        Ok(())
    }
}

type RequestSender = Sender<(comm::Request, ResponseCallback)>;
type RequestReceiver = Receiver<(comm::Request, ResponseCallback)>;

fn run_dispatcher(sock: UnixStream, requester: RequestReceiver) -> Result<()> {
    let mut read_sock = sock.try_clone()?;
    let dispatcher = Mutex::new(Dispatcher::new(sock));

    thread::scope(|scope| -> Result<()> {
        scope.spawn(|| -> Result<()> {
            loop {
                match proto::deserialize_from::<_, comm::Message<comm::Response>>(&mut read_sock) {
                    Ok(message) => dispatcher.lock().unwrap().get_message(message)?,
                    Err(err) => {
                        dispatcher.lock().unwrap().drain(
                            anyhow::Error::from(err).context("local deserialization failed"),
                        );
                        break;
                    }
                }
            }
            Ok(())
        });

        while let Ok((request, sender)) = requester.recv() {
            dispatcher.lock().unwrap().send_message(request, sender)?
        }
        dispatcher.lock().unwrap().stop()?;

        Ok(())
    })
}

macro_rules! send_async {
    ($self:expr, $msg:ident) => { send_async!($self, $msg, ) };
    ($self:expr, $msg:ident, $($arg_n:ident: $arg_v:expr),*) => {{
        let (send, recv) = channel();
        $self.requester.as_ref().unwrap().send((
            comm::Request::$msg { $($arg_n: $arg_v),* },
            Box::new(move |message: comm::Response| {
                match message {
                    comm::Response::$msg(res) => {
                        let _ = send.send(res.map_err(|e| e.into()));
                    },
                    comm::Response::Error(e) => {
                        let _ = send.send(Err(e.into()));
                    },
                    _ => {
                        let _ = send.send(Err(anyhow!("unexpected response: {message:?}")));
                    }
                }
            }),
        )).map(|_| recv).map_err(anyhow::Error::from)
    }};
}

macro_rules! send_sync {
    ($self:expr, $msg:ident) => { send_sync!($self, $msg, ) };
    ($self:expr, $msg:ident, $($arg_n:ident: $arg_v: expr),*) => {{
        (|| -> Result<_> { send_async!($self, $msg, $($arg_n: $arg_v),*)?.recv()? })()
    }};
}

impl Drop for Client {
    fn drop(&mut self) {
        drop(self.requester.take());
        let dispatcher_errored = print_error(
            "dispatcher",
            self.dispatcher_handle.take().unwrap().join().unwrap(),
        );
        let proccess_error = self.process_handle.take().unwrap().join().unwrap();

        if dispatcher_errored {
            print_error("process", proccess_error);
        }
    }
}

pub struct Client {
    requester: Option<RequestSender>,
    process_handle: Option<thread::JoinHandle<Result<()>>>,
    dispatcher_handle: Option<thread::JoinHandle<Result<()>>>,
}

impl Client {
    pub fn new(
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let (sock1, sock2) = UnixStream::pair()?;

        let (send, recv) = channel();
        let process_handle = Some(thread::spawn(move || run_process_client(sock2)));
        let dispatcher_handle = Some(thread::spawn(move || run_dispatcher(sock1, recv)));
        let s = Self {
            requester: Some(send),
            process_handle,
            dispatcher_handle,
        };

        send_sync!(s, Start,
            driver_mode: driver_mode,
            broker_addr: broker_addr,
            project_dir: project_dir.as_ref().to_owned(),
            cache_dir: cache_dir.as_ref().to_owned()
        )?;
        Ok(s)
    }

    pub fn add_artifact(&mut self, path: &Path) -> Result<Sha256Digest> {
        send_sync!(self, AddArtifact, path: path.to_owned())
    }

    pub fn add_layer(&mut self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        send_sync!(self, AddLayer, layer: layer)
    }

    pub fn get_container_image(
        &mut self,
        name: &str,
        tag: &str,
        _prog: ProgressBar,
    ) -> Result<ContainerImage> {
        send_sync!(self, GetContainerImage, name: name.into(), tag: tag.into())
    }

    pub fn add_job(&mut self, spec: JobSpec, handler: JobResponseHandler) -> Result<()> {
        self.requester.as_ref().unwrap().send((
            comm::Request::AddJob { spec },
            Box::new(move |message: comm::Response| {
                if let comm::Response::AddJob(Ok((cjid, result))) = message {
                    handler(cjid, result)
                }
            }),
        ))?;
        Ok(())
    }

    pub fn stop_accepting(&mut self) -> Result<()> {
        send_sync!(self, StopAccepting)
    }

    pub fn wait_for_outstanding_jobs(&mut self) -> Result<()> {
        send_sync!(self, WaitForOutstandingJobs)
    }

    pub fn get_job_state_counts(&mut self) -> Result<Receiver<Result<JobStateCounts>>> {
        send_async!(self, GetJobStateCounts)
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_broker_msg_single_threaded(&self, count: usize) {
        send_sync!(self, ProcessBrokerMsgSingleThreaded, count: count).unwrap()
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_client_messages_single_threaded(&self) {
        send_sync!(self, ProcessClientMessagesSingleThreaded).unwrap()
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_artifact_single_threaded(&self) {
        send_sync!(self, ProcessArtifactSingleThreaded).unwrap()
    }
}
