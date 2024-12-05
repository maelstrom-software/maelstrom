pub use maelstrom_client_base::{
    container_spec, image_container_parent, job_spec, spec,
    spec::{ContainerParent, ContainerSpec, JobSpec},
    AcceptInvalidRemoteContainerTlsCerts, ArtifactUploadStrategy, CacheDir, IntrospectResponse,
    JobRunningStatus, JobStatus, ProjectDir, RemoteProgress, RpcLogMessage, StateDir, MANIFEST_DIR,
};
pub use maelstrom_container::ContainerImageDepotDir;

use anyhow::{anyhow, Context as _, Result};
use futures::stream::StreamExt as _;
use maelstrom_base::{ClientJobId, JobOutcomeResult};
use maelstrom_client_base::{
    proto::{self, client_process_client::ClientProcessClient},
    AddContainerRequest, IntoProtoBuf, StartRequest, TryFromProtoBuf,
};
use maelstrom_linux::{self as linux, Pid};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    root::Root,
};
use std::{
    future::Future,
    io::{BufRead as _, BufReader, Read as _},
    net::Shutdown,
    os::linux::net::SocketAddrExt as _,
    os::unix::net::{SocketAddr, UnixListener, UnixStream as StdUnixStream},
    path::Path,
    pin::Pin,
    process,
    process::{Command, Stdio},
    result, str,
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

fn print_error(label: &str, res: Result<()>) {
    if let Err(e) = res {
        eprintln!("{label}: error: {e:?}");
    }
}

#[derive(Debug)]
struct ClientBgHandle(Pid);

impl ClientBgHandle {
    fn wait(&mut self) -> Result<()> {
        linux::waitpid(self.0).map_err(|e| anyhow!("waitpid failed: {e}"))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ClientBgProcess {
    handle: ClientBgHandle,
    sock: Option<StdUnixStream>,
}

impl ClientBgProcess {
    pub fn new_from_fork(log_level: LogLevel) -> Result<Self> {
        let (sock1, sock2) = StdUnixStream::pair()?;
        if let Some(pid) = linux::fork().context("forking client background process")? {
            Ok(Self {
                handle: ClientBgHandle(pid),
                sock: Some(sock1),
            })
        } else {
            drop(sock1);
            maelstrom_client_process::main(sock2, None, log_level)
                .context("client background process")?;
            process::exit(0);
        }
    }

    pub fn new_from_bin(bin_path: &Path, args: &[&str]) -> Result<Self> {
        let mut proc = Command::new(bin_path)
            .args(args)
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stderr = proc.stderr.take().unwrap();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                println!("client bg-process: {line}");
            }
        });
        let mut address_bytes = [0; 5]; // We know Linux uses exactly 5 bytes for this
        proc.stdout.take().unwrap().read_exact(&mut address_bytes)?;
        let address = SocketAddr::from_abstract_name(address_bytes)?;
        let sock = StdUnixStream::connect_addr(&address)
            .with_context(|| format!("failed to connect to {address:?}"))?;
        Ok(Self {
            handle: ClientBgHandle(proc.into()),
            sock: Some(sock),
        })
    }

    fn take_socket(&mut self) -> StdUnixStream {
        self.sock.take().unwrap()
    }

    fn wait(&mut self) -> Result<()> {
        self.handle.wait()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        slog::debug!(self.log, "dropping Client");
        drop(self.requester.take());
        slog::debug!(self.log, "Client::drop: waiting for dispatcher");
        print_error(
            "dispatcher",
            self.dispatcher_handle.take().unwrap().join().unwrap(),
        );
        slog::debug!(
            self.log,
            "Client::drop: waiting for child process";
            "process_handle" => ?self.process_handle
        );
        self.process_handle.wait().unwrap();
    }
}

pub struct Client {
    requester: Option<RequestSender>,
    process_handle: ClientBgProcess,
    dispatcher_handle: Option<thread::JoinHandle<Result<()>>>,
    log: slog::Logger,
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
    log: &slog::Logger,
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
        mut process_handle: ClientBgProcess,
        broker_addr: Option<BrokerAddr>,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        container_image_depot_dir: impl AsRef<Root<ContainerImageDepotDir>>,
        cache_dir: impl AsRef<Root<CacheDir>>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
        artifact_upload_strategy: ArtifactUploadStrategy,
        log: slog::Logger,
    ) -> Result<Self> {
        let (send, recv) = tokio_mpsc::unbounded_channel();

        let sock = process_handle.take_socket();
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
            artifact_upload_strategy,
        };
        let s = Self {
            requester: Some(send),
            process_handle,
            dispatcher_handle: Some(dispatcher_handle),
            log,
            start_req,
        };

        slog::debug!(s.log, "opening log stream");
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
        slog::debug!(self.log, "client sending start"; "request" => ?self.start_req);

        let req = self.start_req.clone().into_proto_buf();
        self.send_sync_unit(|mut client| async move { client.start(req).await })?;
        slog::debug!(self.log, "client completed start");
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

pub fn bg_proc_main() -> Result<()> {
    maelstrom_client_process::clone_into_pid_and_user_namespace()?;

    maelstrom_util::log::run_with_logger(maelstrom_util::config::common::LogLevel::Debug, |log| {
        let (sock, path) = linux::autobound_unix_listener(Default::default(), 1)?;
        let name = str::from_utf8(&path[1..]).unwrap();

        slog::info!(log, "listening on unix-abstract:{name}");
        println!("{name}");

        let (sock, addr) = UnixListener::from(sock).accept()?;
        slog::info!(log, "got connection"; "address" => ?addr);

        let res = maelstrom_client_process::main_after_clone(
            sock,
            Some(log.clone()),
            maelstrom_util::config::common::LogLevel::Debug,
        );
        slog::info!(log, "shutting down"; "res" => ?res);
        res
    })
}
