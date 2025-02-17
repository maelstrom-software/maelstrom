pub use maelstrom_client_base::{
    container_container_parent, container_ref, container_spec, converted_image, environment_spec,
    glob_layer_spec, image_container_parent, image_ref, job_spec, paths_layer_spec, prefix_options,
    shared_library_dependencies_layer_spec, spec, stubs_layer_spec, symlink_spec,
    symlinks_layer_spec, tar_layer_spec, AcceptInvalidRemoteContainerTlsCerts, CacheDir,
    IntrospectResponse, JobRunningStatus, JobStatus, ProjectDir, RemoteProgress, RpcLogMessage,
    StateDir, MANIFEST_DIR,
};
pub use maelstrom_container::ContainerImageDepotDir;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::stream::StreamExt as _;
use maelstrom_base::{ClientJobId, JobOutcomeResult};
use maelstrom_client_base::{
    proto::{self, client_process_client::ClientProcessClient},
    spec::{ContainerSpec, JobSpec},
    AddContainerRequest, IntoProtoBuf, StartRequest, TryFromProtoBuf,
};
use maelstrom_linux::{self as linux, Pid};
use maelstrom_util::{
    config::common::{
        ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots,
    },
    root::Root,
};
use slog::{debug, warn, Logger};
use std::{
    cell::Cell,
    ffi::{OsStr, OsString},
    future::Future,
    io::{BufRead as _, BufReader, Read as _},
    net::Shutdown,
    os::{
        linux::net::SocketAddrExt as _,
        unix::net::{SocketAddr, UnixStream as StdUnixStream},
    },
    pin::Pin,
    process::{self, Command, Stdio},
    result,
    sync::mpsc::{self as std_mpsc, Receiver},
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

#[tokio::main]
async fn run_dispatcher(std_sock: StdUnixStream, mut requester: RequestReceiver) -> Result<()> {
    std_sock.set_nonblocking(true)?;
    let sock = TokioUnixStream::from_std(std_sock.try_clone()?)?;
    let mut closure = Some(move || async move { result::Result::<_, tower::BoxError>::Ok(sock) });
    let channel = tonic::transport::Endpoint::try_from("http://[::]")?
        .connect_with_connector(tower::service_fn(move |_| {
            (closure.take().expect("unexpected reconnect"))()
        }))
        .await?;

    while let Some(f) = requester.recv().await {
        task::spawn(f(ClientProcessClient::new(channel.clone())));
    }

    std_sock.shutdown(Shutdown::Both)?;

    Ok(())
}

pub trait ClientProcessFactory {
    fn new_client_process(&self, log: &Logger) -> Result<(ClientProcessHandle, StdUnixStream)>;
}

pub struct ForkClientProcessFactory {
    handle: Cell<Option<(ClientProcessHandle, StdUnixStream)>>,
}

impl ForkClientProcessFactory {
    pub fn new(log_level: LogLevel) -> Result<Self> {
        let (sock1, sock2) = StdUnixStream::pair()?;
        if let Some(pid) = linux::fork().context("forking client background process")? {
            Ok(Self {
                handle: Cell::new(Some((ClientProcessHandle { pid, log: None }, sock1))),
            })
        } else {
            drop(sock1);
            let exit_code = maelstrom_client_process::main_for_fork(sock2, log_level)
                .context("client background process")?;
            process::exit(exit_code.into());
        }
    }
}

impl ClientProcessFactory for ForkClientProcessFactory {
    fn new_client_process(&self, log: &Logger) -> Result<(ClientProcessHandle, StdUnixStream)> {
        let Some((mut handle, socket)) = self.handle.replace(None) else {
            bail!("can only create one client process with this factory")
        };
        handle.log.replace(log.clone());
        Ok((handle, socket))
    }
}

pub struct SpawnClientProcessFactory {
    program: OsString,
    args: Vec<OsString>,
}

impl SpawnClientProcessFactory {
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
        Ok((
            ClientProcessHandle {
                pid: proc.into(),
                log: Some(log.clone()),
            },
            sock,
        ))
    }
}

/// A struct of this type is required to create a [`Client`]. It represents a background client
/// process, running the code in the [`maelstrom-client-process`] crate.
///
/// The client library does most of its work by sending RPCs to the client process, which then
/// connects to the broker, runs local jobs, etc. There are two ways to get a client process
/// running. The first is to fork without exec-ing. The second is to fork and then exec (also
/// called spawn).
///
/// The advantage of the first approach, a fork without an exec, is that it doesn't require a
/// separate binary be deployed along with the client library. The program using the library just
/// packages up the client process code in its binary, and runs that in a separate process.
///
/// The main downside of the first approach is that the client process needs to be created while
/// the process is single-threaded. As a result, the client code needs to create one of the
/// [`ClientProcess`]es early during startup, and then pass it down to the [`Client`] when it's
/// created.
///
/// The advantage of the second approach is that client processes can be created at any time, even
/// after the process is multi-threaded. However, the downside is that the client code must know
/// where the client process executable is located on the local file system.
#[derive(Debug)]
pub struct ClientProcessHandle {
    pid: Pid,
    log: Option<Logger>,
}

impl Drop for ClientProcessHandle {
    fn drop(&mut self) {
        if let Some(log) = self.log.as_ref() {
            debug!(log, "waiting for child client process"; "pid" => ?self.pid);
        }
        if let Err(errno) = linux::waitpid(self.pid) {
            if let Some(log) = self.log.as_ref() {
                warn!(log, "error waiting for child client process"; "errno" => %errno);
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!(self.log, "dropping Client");
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

    /// Kills all running jobs and clears the layer caches.
    pub fn restart(&self) -> Result<()> {
        self.send_sync_unit(move |mut client| async move { client.restart(proto::Void {}).await })?;
        self.send_start()?;
        Ok(())
    }
}
