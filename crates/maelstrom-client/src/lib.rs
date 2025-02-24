pub use maelstrom_client_base::{
    container_container_parent, container_ref, container_spec, converted_image, environment_spec,
    glob_layer_spec, image_container_parent, image_ref, job_spec, paths_layer_spec, prefix_options,
    shared_library_dependencies_layer_spec, spec, stubs_layer_spec, symlink_spec,
    symlinks_layer_spec, tar_layer_spec, AcceptInvalidRemoteContainerTlsCerts, CacheDir,
    IntrospectResponse, JobRunningStatus, JobStatus, ProjectDir, RemoteProgress, RpcLogMessage,
    StateDir, MANIFEST_DIR,
};
pub use maelstrom_container::ContainerImageDepotDir;

use anyhow::{anyhow, Context as _, Error, Result};
use futures::stream::StreamExt as _;
use http::Uri;
use hyper_util::rt::TokioIo;
use maelstrom_base::{ClientJobId, JobOutcomeResult};
use maelstrom_client_base::{
    proto::{self, client_process_client::ClientProcessClient},
    spec::{ContainerSpec, JobSpec},
    AddContainerRequest, IntoProtoBuf, StartRequest, TryFromProtoBuf,
};
use maelstrom_linux::{self as linux, Fd, Pid, Signal, UnixStream};
use maelstrom_util::{
    config::common::{
        ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots,
    },
    process::assert_single_threaded,
    root::Root,
};
use slog::{debug, warn, Logger};
use std::{
    ffi::{OsStr, OsString},
    future::Future,
    io::{BufRead as _, BufReader, Read as _},
    net::Shutdown,
    os::{
        fd::AsRawFd as _,
        linux::net::SocketAddrExt as _,
        unix::net::{SocketAddr, UnixStream as StdUnixStream},
    },
    pin::Pin,
    process::{self, Command, Stdio},
    result,
    sync::{
        mpsc::{self as std_mpsc, Receiver},
        Arc, Mutex,
    },
    task::{Context, Poll},
    thread,
};
use tokio::{
    net::UnixStream as TokioUnixStream,
    sync::mpsc::{self as tokio_mpsc, UnboundedReceiver, UnboundedSender},
    task,
};

type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type RequestFn = Box<
    dyn FnOnce(ClientProcessClient<tonic::transport::Channel>) -> BoxedFuture
        + Send
        + Sync
        + 'static,
>;
type RequestReceiver = UnboundedReceiver<RequestFn>;
type RequestSender = UnboundedSender<RequestFn>;

type TonicResult<T> = result::Result<T, tonic::Status>;
type TonicResponse<T> = TonicResult<tonic::Response<T>>;

struct ConnectService<T>(Arc<Mutex<Option<T>>>);

impl<T> ConnectService<T> {
    fn new(inner: T) -> Self {
        Self(Arc::new(Mutex::new(Some(inner))))
    }
}

impl<T> tower::Service<Uri> for ConnectService<T> {
    type Response = T;
    type Error = Error;
    type Future = Self;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        Self(self.0.clone())
    }
}

impl<T> Future for ConnectService<T> {
    type Output = Result<T, Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            self.0
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| anyhow!("client disconnected")),
        )
    }
}

#[tokio::main]
async fn run_dispatcher(std_sock: StdUnixStream, mut requester: RequestReceiver) -> Result<()> {
    std_sock.set_nonblocking(true)?;
    let sock = TokioUnixStream::from_std(std_sock.try_clone()?)?;
    let sock = TokioIo::new(sock);
    let service = ConnectService::new(sock);
    let channel = tonic::transport::Endpoint::try_from("http://[::]")?
        .connect_with_connector(service)
        .await?;

    while let Some(f) = requester.recv().await {
        task::spawn(f(ClientProcessClient::new(channel.clone())));
    }

    std_sock.shutdown(Shutdown::Both)?;

    Ok(())
}

/// A type that implements this trait is required to create a a [`Client`]. It represents a factory
/// that can create a client process, which will run the code in [`maelstrom_client_process`].
///
/// The client library does most of its work by sending RPCs to the client process, which then
/// connects to the broker, runs local jobs, etc.
///
/// There are two ways to start a client process. The first is to fork without exec-ing
/// ([`ForkClientProcessFactory`]). The second is to fork and then exec (also called "spawning",
/// [`SpawnClientProcessFactory`]).
///
/// The advantage of a fork without an exec is that it doesn't require a separate binary to be
/// deployed along with the client library. The program using the library just forks a separate
/// process, which jumps to the client process code that is packaged up in the client library.
///
/// The main disadvantage of a fork without an exec is that the [`ForkClientProcessFactory`] must
/// be created while the process is single threaded. If that's not possible, the
/// [`SpawnClientProcessFactory`] can be used instead. The disadvantage of
/// [`SpawnClientProcessFactory`] is that it requires a client process executable to be located on
/// the local file system.
///
/// The [`SpawnClientProcessFactory`] can also be used to test the stand-alone client process,
/// which may be used by bindings for other languages.
pub trait ClientProcessFactory {
    fn new_client_process(&self, log: &Logger) -> Result<(ClientProcessHandle, StdUnixStream)>;
}

/// Create client processes by forking without exec-ing. When a [`ForkClientProcessFactory`] is
/// started, a "zygote" process is forked. That zygote process then forks client processes when
/// requested.
pub struct ForkClientProcessFactory {
    pid: Pid,
    socket: UnixStream,
}

impl ForkClientProcessFactory {
    /// Create a new [`ForkClientProcessFactory`]. This must be called while the process is still
    /// single-threaded.
    pub fn new(log_level: LogLevel) -> Result<Self> {
        assert_single_threaded()?;
        let (sock1, sock2) = UnixStream::pair()?;
        if let Some(pid) = linux::fork().context("forking client process zygote process")? {
            Ok(Self { pid, socket: sock1 })
        } else {
            drop(sock1);
            let exit_code = maelstrom_client_process::main_for_zygote(sock2, log_level)
                .context("client process zygote")?;
            process::exit(exit_code.into());
        }
    }
}

impl Drop for ForkClientProcessFactory {
    fn drop(&mut self) {
        let _ = linux::kill(self.pid, Signal::KILL);
        let _ = linux::waitpid(self.pid);
    }
}

impl ClientProcessFactory for ForkClientProcessFactory {
    fn new_client_process(&self, _log: &Logger) -> Result<(ClientProcessHandle, StdUnixStream)> {
        let (sock1, sock2) = StdUnixStream::pair()?;
        let buf = [0; 1];
        self.socket
            .send_with_fd(&buf, Fd::from_raw(sock2.as_raw_fd()))?;
        Ok((ClientProcessHandle(None), sock1))
    }
}

/// Create client processes by spawning (forking and exec-ing).
pub struct SpawnClientProcessFactory {
    program: OsString,
    args: Vec<OsString>,
}

impl SpawnClientProcessFactory {
    /// Create a [`SpawnClientProcessFactory`] that forks and executes the program specified by
    /// `program` and `args`.
    pub fn new<ProgramT, ArgsT, ArgT>(program: ProgramT, args: ArgsT) -> Self
    where
        ProgramT: AsRef<OsStr>,
        ArgsT: IntoIterator<Item = ArgT>,
        ArgT: AsRef<OsStr>,
    {
        Self {
            program: program.as_ref().to_owned(),
            args: args
                .into_iter()
                .map(|arg| arg.as_ref().to_owned())
                .collect(),
        }
    }
}

impl ClientProcessFactory for SpawnClientProcessFactory {
    fn new_client_process(&self, log: &Logger) -> Result<(ClientProcessHandle, StdUnixStream)> {
        let mut proc = Command::new(&self.program)
            .args(&self.args)
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stderr = proc.stderr.take().unwrap();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                println!("client process: {line}");
            }
        });
        let mut address_bytes = [0; 5]; // We know Linux uses exactly 5 bytes for this
        proc.stdout.take().unwrap().read_exact(&mut address_bytes)?;
        let address = SocketAddr::from_abstract_name(address_bytes)?;
        let sock = StdUnixStream::connect_addr(&address)
            .with_context(|| format!("failed to connect to {address:?}"))?;
        Ok((ClientProcessHandle(Some((proc.into(), log.clone()))), sock))
    }
}

pub struct ClientProcessHandle(Option<(Pid, Logger)>);

impl Drop for ClientProcessHandle {
    fn drop(&mut self) {
        if let ClientProcessHandle(Some((pid, log))) = self {
            debug!(log, "waiting for child client process"; "pid" => ?pid);
            if let Err(errno) = linux::waitpid(*pid) {
                warn!(log, "error waiting for child client process"; "errno" => %errno);
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!(self.log, "dropping Client: waiting for stop request");
        if let Err(err) =
            self.send_sync_unit(move |mut client| async move { client.stop(proto::Void {}).await })
        {
            debug!(self.log, "dropping Client: error waiting for stop: {err}");
        }
        drop(self.requester.take());
        debug!(self.log, "Client::drop: waiting for dispatcher");
        if let Err(err) = self.dispatcher_handle.take().unwrap().join().unwrap() {
            warn!(self.log, "dispatcher task returned error"; "error" => %err);
        }
    }
}

pub struct Client {
    requester: Option<RequestSender>,
    _client_process_handle: ClientProcessHandle,
    dispatcher_handle: Option<thread::JoinHandle<Result<()>>>,
    log: Logger,
    start_req: StartRequest,
}

fn map_tonic_error(error: tonic::Status) -> anyhow::Error {
    // We use this error code to serialize application errors.
    if error.code() == tonic::Code::Unknown {
        anyhow::Error::msg(format!("Client process error:\n{}", error.message()))
    } else {
        error.into()
    }
}

fn transform_rpc_response<RetT: TryFromProtoBuf>(
    res: TonicResponse<<RetT as TryFromProtoBuf>::ProtoBufType>,
) -> Result<RetT> {
    TryFromProtoBuf::try_from_proto_buf(res.map_err(map_tonic_error)?.into_inner())
}

async fn handle_log_messages(
    log: &Logger,
    mut stream: tonic::Streaming<proto::LogMessage>,
) -> Result<()> {
    while let Some(message) = stream.next().await {
        let message = RpcLogMessage::try_from_proto_buf(message.map_err(map_tonic_error)?)?;
        message.log_to(log);
    }
    Ok(())
}

fn wait_for_job_completed_blocking(
    receiver: std_mpsc::Receiver<Result<JobStatus>>,
) -> Result<(ClientJobId, JobOutcomeResult)> {
    loop {
        if let JobStatus::Completed {
            client_job_id,
            result,
        } = receiver.recv().map_err(|_| anyhow!("job canceled"))??
        {
            break Ok((client_job_id, result));
        }
    }
}

impl Client {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_process_factory: &impl ClientProcessFactory,
        broker_addr: Option<BrokerAddr>,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        container_image_depot_dir: impl AsRef<Root<ContainerImageDepotDir>>,
        cache_dir: impl AsRef<Root<CacheDir>>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
        artifact_transfer_strategy: ArtifactTransferStrategy,
        log: Logger,
    ) -> Result<Self> {
        let (client_process_handle, sock) = client_process_factory.new_client_process(&log)?;
        let (send, recv) = tokio_mpsc::unbounded_channel();
        let dispatcher_handle = thread::spawn(move || run_dispatcher(sock, recv));

        let start_req = StartRequest {
            broker_addr,
            project_dir: project_dir.as_ref().to_owned(),
            state_dir: state_dir.as_ref().to_owned(),
            cache_dir: cache_dir.as_ref().to_owned(),
            container_image_depot_dir: container_image_depot_dir.as_ref().to_owned(),
            cache_size,
            inline_limit,
            slots,
            accept_invalid_remote_container_tls_certs,
            artifact_transfer_strategy,
        };
        let s = Self {
            requester: Some(send),
            _client_process_handle: client_process_handle,
            dispatcher_handle: Some(dispatcher_handle),
            log,
            start_req,
        };

        debug!(s.log, "opening log stream");
        let rpc_log = s.log.clone();
        s.send_sync_unit(|mut client| async move {
            let log_stream = client
                .stream_log_messages(proto::Void {})
                .await?
                .into_inner();
            tokio::task::spawn(async move {
                let _ = handle_log_messages(&rpc_log, log_stream).await;
            });
            Ok(tonic::Response::new(proto::Void {}))
        })?;

        s.send_start()?;

        Ok(s)
    }

    fn send_start(&self) -> Result<()> {
        debug!(self.log, "client sending start"; "request" => ?self.start_req);

        let req = self.start_req.clone().into_proto_buf();
        self.send_sync_unit(|mut client| async move { client.start(req).await })?;
        debug!(self.log, "client completed start");
        Ok(())
    }

    fn send_async<BuilderT, FutureT, RetT>(
        &self,
        builder: BuilderT,
    ) -> Result<Receiver<Result<RetT>>>
    where
        BuilderT: FnOnce(ClientProcessClient<tonic::transport::Channel>) -> FutureT,
        BuilderT: Send + Sync + 'static,
        FutureT: Future<
                Output = result::Result<
                    tonic::Response<<RetT as TryFromProtoBuf>::ProtoBufType>,
                    tonic::Status,
                >,
            > + Send,
        RetT: TryFromProtoBuf + Send + 'static,
    {
        let (send, recv) = std_mpsc::channel();
        self.requester
            .as_ref()
            .unwrap()
            .send(Box::new(move |client| {
                Box::pin(async move {
                    let _ = send.send(transform_rpc_response(builder(client).await));
                })
            }))
            .with_context(|| "sending RPC request to client process")?;
        Ok(recv)
    }

    fn send_sync<BuilderT, FutureT, RetT>(&self, builder: BuilderT) -> Result<RetT>
    where
        BuilderT: FnOnce(ClientProcessClient<tonic::transport::Channel>) -> FutureT,
        BuilderT: Send + Sync + 'static,
        FutureT: Future<
                Output = result::Result<
                    tonic::Response<<RetT as TryFromProtoBuf>::ProtoBufType>,
                    tonic::Status,
                >,
            > + Send,
        RetT: TryFromProtoBuf + Send + 'static,
    {
        self.send_async(builder)?
            .recv()
            .with_context(|| "receiving RPC response from client process")?
    }

    /// This is a less confusing way to write `self.send_sync::<_, _, ()>`
    fn send_sync_unit<BuilderT, FutureT>(&self, builder: BuilderT) -> Result<()>
    where
        BuilderT: FnOnce(ClientProcessClient<tonic::transport::Channel>) -> FutureT,
        BuilderT: Send + Sync + 'static,
        FutureT:
            Future<Output = result::Result<tonic::Response<proto::Void>, tonic::Status>> + Send,
    {
        self.send_sync(builder)
    }

    pub fn add_job(
        &self,
        spec: JobSpec,
        mut handler: impl FnMut(Result<JobStatus>) + Send + Sync + Clone + 'static,
    ) -> Result<()> {
        let msg = proto::RunJobRequest {
            spec: Some(spec.clone().into_proto_buf()),
        };
        self.requester
            .as_ref()
            .unwrap()
            .send(Box::new(move |mut client| {
                Box::pin(async move {
                    let mut stream = match client.run_job(msg).await.map_err(map_tonic_error) {
                        Ok(v) => v.into_inner(),
                        Err(e) => {
                            let _ = task::spawn_blocking(move || handler(Err(e))).await.ok();
                            return;
                        }
                    };
                    while let Some(status) = stream.next().await {
                        let status = async {
                            JobStatus::try_from_proto_buf(status.map_err(map_tonic_error)?)
                        }
                        .await;
                        let was_error = status.is_err();
                        let mut handler = handler.clone();
                        if task::spawn_blocking(move || handler(status)).await.is_err() {
                            break;
                        }
                        if was_error {
                            break;
                        }
                    }
                })
            }))?;
        Ok(())
    }

    pub fn run_job(&self, spec: JobSpec) -> Result<(ClientJobId, JobOutcomeResult)> {
        let (sender, receiver) = std_mpsc::channel();
        self.add_job(spec, move |result| {
            let _ = sender.send(result);
        })?;
        wait_for_job_completed_blocking(receiver)
    }

    pub fn add_container(&self, name: String, container: ContainerSpec) -> Result<()> {
        self.send_sync(|mut client| async move {
            client
                .add_container(AddContainerRequest { name, container }.into_proto_buf())
                .await
        })
    }

    pub fn introspect(&self) -> Result<IntrospectResponse> {
        self.send_sync(move |mut client| async move { client.introspect(proto::Void {}).await })
    }
}
