pub mod test;

pub use maelstrom_client_base::{spec, ClientDriverMode, JobResponseHandler, MANIFEST_DIR};

use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use maelstrom_base::{proto, stats::JobStateCounts, ArtifactType, JobSpec, Sha256Digest};
use maelstrom_client_base::comm;
use maelstrom_container::ContainerImage;
use maelstrom_util::config::BrokerAddr;
use spec::Layer;
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use std::thread;

fn print_error(label: &str, res: Result<()>) {
    if let Err(e) = res {
        eprintln!("{label}: error: {e:?}");
    }
}

type ResponseCallback = Box<dyn FnMut(comm::Response) + Send + Sync>;

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
            .get_mut(&message.id)
            .expect("unexpected message");
        callback(message.body);
        if message.finished {
            self.outstanding.remove(&message.id);
        }

        Ok(())
    }

    fn send_message(
        &mut self,
        request: comm::Request,
        mut callback: ResponseCallback,
    ) -> Result<()> {
        if let Some(err) = &self.drained_error {
            callback(comm::Response::Error(err.clone()));
            return Ok(());
        }

        self.outstanding.insert(self.next_message_id, callback);
        proto::serialize_into(
            &mut self.sock,
            &comm::Message {
                id: self.next_message_id,
                finished: false,
                body: request,
            },
        )?;
        self.next_message_id = self.next_message_id.next();
        Ok(())
    }

    fn drain(&mut self, error: impl Into<comm::CommunicationError>) {
        let err = error.into();
        for (_, mut callback) in self.outstanding.drain() {
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
            #[allow(clippy::redundant_field_names)]
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

fn run_progress_bar<Ret>(
    prog: ProgressBar,
    recv: Receiver<Result<comm::ProgressResponse<Ret>>>,
) -> Result<Ret> {
    for msg in recv.iter() {
        match msg? {
            comm::ProgressResponse::InProgress(comm::ProgressInfo::Length(len)) => {
                prog.set_length(len)
            }
            comm::ProgressResponse::InProgress(comm::ProgressInfo::Inc(v)) => prog.inc(v),
            comm::ProgressResponse::Done(ret) => return Ok(ret),
        }
    }
    Err(anyhow!("call incomplete"))
}

enum ClientBgHandle {
    Pid(maelstrom_linux::Pid),
    Thread(Option<thread::JoinHandle<Result<()>>>),
}

impl ClientBgHandle {
    fn wait(&mut self) -> Result<()> {
        match self {
            Self::Pid(pid) => {
                maelstrom_linux::waitpid(*pid).map_err(|e| anyhow!("waitpid failed: {e}"))?;
            }
            Self::Thread(handle) => print_error("process", handle.take().unwrap().join().unwrap()),
        }
        Ok(())
    }
}

pub struct ClientBgProcess {
    handle: ClientBgHandle,
    sock: Option<UnixStream>,
}

impl ClientBgProcess {
    pub fn new_from_fork() -> Result<Self> {
        let (sock1, sock2) = UnixStream::pair()?;
        if let Some(pid) = maelstrom_linux::fork().map_err(|e| anyhow!("fork failed: {e}"))? {
            Ok(Self {
                handle: ClientBgHandle::Pid(pid),
                sock: Some(sock1),
            })
        } else {
            let _ = maelstrom_client_process::run_process_client(sock2);
            std::process::exit(0)
        }
    }

    pub fn new_from_thread() -> Result<Self> {
        let (sock1, sock2) = UnixStream::pair()?;
        let handle = thread::spawn(move || maelstrom_client_process::run_process_client(sock1));
        Ok(Self {
            handle: ClientBgHandle::Thread(Some(handle)),
            sock: Some(sock2),
        })
    }

    fn take_socket(&mut self) -> UnixStream {
        self.sock.take().unwrap()
    }

    fn wait(&mut self) -> Result<()> {
        self.handle.wait()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        drop(self.requester.take());
        print_error(
            "dispatcher",
            self.dispatcher_handle.take().unwrap().join().unwrap(),
        );
        self.process_handle.wait().unwrap();
    }
}

pub struct Client {
    requester: Option<RequestSender>,
    process_handle: ClientBgProcess,
    dispatcher_handle: Option<thread::JoinHandle<Result<()>>>,
}

impl Client {
    pub fn new(
        mut process_handle: ClientBgProcess,
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let (send, recv) = channel();

        let sock = process_handle.take_socket();
        let dispatcher_handle = thread::spawn(move || run_dispatcher(sock, recv));
        let s = Self {
            requester: Some(send),
            process_handle,
            dispatcher_handle: Some(dispatcher_handle),
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
        prog: ProgressBar,
    ) -> Result<ContainerImage> {
        let resp = send_async!(self, GetContainerImage, name: name.into(), tag: tag.into())?;
        run_progress_bar(prog, resp)
    }

    pub fn add_job(&mut self, spec: JobSpec, handler: JobResponseHandler) -> Result<()> {
        let mut once_handler = Some(handler);
        self.requester.as_ref().unwrap().send((
            comm::Request::AddJob { spec },
            Box::new(move |message: comm::Response| {
                if let comm::Response::AddJob(Ok((cjid, result))) = message {
                    (once_handler.take().unwrap())(cjid, result)
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
