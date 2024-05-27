pub use maelstrom_client_base::{
    spec, CacheDir, IntrospectResponse, ProjectDir, RemoteProgress, StateDir, MANIFEST_DIR,
};
pub use maelstrom_container::ContainerImageDepotDir;

use anyhow::{anyhow, bail, Context as _, Result};
use maelstrom_base::{ArtifactType, ClientJobId, JobOutcomeResult, Sha256Digest};
use maelstrom_client_base::{
    proto::{self, client_process_client::ClientProcessClient},
    spec::JobSpec,
    IntoProtoBuf, IntoResult, TryFromProtoBuf,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    log::LoggerFactory,
    root::Root,
};
use spec::Layer;
use std::os::linux::net::SocketAddrExt as _;
use std::{
    future::Future,
    io::{BufRead as _, BufReader},
    os::unix::net::{SocketAddr, UnixListener, UnixStream},
    path::Path,
    pin::Pin,
    process,
    process::{Command, Stdio},
    thread,
};

type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type RequestFn = Box<
    dyn FnOnce(ClientProcessClient<tonic::transport::Channel>) -> BoxedFuture
        + Send
        + Sync
        + 'static,
>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<RequestFn>;
type RequestSender = tokio::sync::mpsc::UnboundedSender<RequestFn>;

type TonicResult<T> = std::result::Result<T, tonic::Status>;
type TonicResponse<T> = TonicResult<tonic::Response<T>>;

#[tokio::main]
async fn run_dispatcher(std_sock: UnixStream, mut requester: RequestReceiver) -> Result<()> {
    std_sock.set_nonblocking(true)?;
    let sock = tokio::net::UnixStream::from_std(std_sock.try_clone()?)?;
    let mut closure =
        Some(move || async move { std::result::Result::<_, tower::BoxError>::Ok(sock) });
    let channel = tonic::transport::Endpoint::try_from("http://[::]")?
        .connect_with_connector(tower::service_fn(move |_| {
            (closure.take().expect("unexpected reconnect"))()
        }))
        .await?;

    while let Some(f) = requester.recv().await {
        tokio::spawn(f(ClientProcessClient::new(channel.clone())));
    }

    std_sock.shutdown(std::net::Shutdown::Both)?;

    Ok(())
}

fn print_error(label: &str, res: Result<()>) {
    if let Err(e) = res {
        eprintln!("{label}: error: {e:?}");
    }
}

struct ClientBgHandle(maelstrom_linux::Pid);

impl ClientBgHandle {
    fn wait(&mut self) -> Result<()> {
        maelstrom_linux::waitpid(self.0).map_err(|e| anyhow!("waitpid failed: {e}"))?;
        Ok(())
    }
}

pub struct ClientBgProcess {
    handle: ClientBgHandle,
    sock: Option<UnixStream>,
}

impl ClientBgProcess {
    pub fn new_from_fork(log_level: LogLevel) -> Result<Self> {
        let (sock1, sock2) = UnixStream::pair()?;
        if let Some(pid) = maelstrom_linux::fork().map_err(|e| anyhow!("fork failed: {e}"))? {
            Ok(Self {
                handle: ClientBgHandle(pid),
                sock: Some(sock1),
            })
        } else {
            match maelstrom_client_process::main(sock2, LoggerFactory::FromLevel(log_level)) {
                Ok(()) => process::exit(0),
                Err(err) => {
                    eprintln!("exiting because of error: {err}");
                    process::exit(1);
                }
            }
        }
    }

    pub fn new_from_bin(bin_path: &Path) -> Result<Self> {
        let mut proc = Command::new(bin_path)
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stderr = proc.stderr.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                println!("client bg-process: {line}");
            }
        });
        let mut address = String::new();
        BufReader::new(proc.stdout.take().unwrap()).read_line(&mut address)?;
        let trimmed_address = address.trim();
        if trimmed_address.is_empty() {
            bail!("process didn't return any address")
        }
        let sock =
            UnixStream::connect_addr(&SocketAddr::from_abstract_name(trimmed_address.as_bytes())?)?;
        Ok(Self {
            handle: ClientBgHandle(proc.into()),
            sock: Some(sock),
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
        slog::debug!(self.log, "dropping Client");
        drop(self.requester.take());
        slog::debug!(self.log, "Client::drop: waiting for dispatcher");
        print_error(
            "dispatcher",
            self.dispatcher_handle.take().unwrap().join().unwrap(),
        );
        slog::debug!(self.log, "Client::drop: waiting for child process");
        self.process_handle.wait().unwrap();
    }
}

pub struct Client {
    requester: Option<RequestSender>,
    process_handle: ClientBgProcess,
    dispatcher_handle: Option<thread::JoinHandle<Result<()>>>,
    log: slog::Logger,
}

fn map_tonic_error(error: tonic::Status) -> anyhow::Error {
    // We use this error code to serialize application errors.
    if error.code() == tonic::Code::Unknown {
        anyhow::Error::msg(format!("Client process error:\n{}", error.message()))
    } else {
        error.into()
    }
}

fn flatten_rpc_result<ProtRetT>(res: TonicResponse<ProtRetT>) -> Result<ProtRetT::Output>
where
    ProtRetT: IntoResult,
{
    res.map_err(map_tonic_error)?.into_inner().into_result()
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
        log: slog::Logger,
    ) -> Result<Self> {
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();

        let sock = process_handle.take_socket();
        let dispatcher_handle = thread::spawn(move || run_dispatcher(sock, recv));
        let s = Self {
            requester: Some(send),
            process_handle,
            dispatcher_handle: Some(dispatcher_handle),
            log,
        };
        slog::debug!(s.log, "finding maelstrom container dir");

        slog::debug!(s.log, "client sending start";
            "broker_addr" => ?broker_addr,
            "project_dir" => ?project_dir.as_ref(),
            "state_dir" => ?state_dir.as_ref(),
            "cache_dir" => ?cache_dir.as_ref(),
            "container_image_depot_cache_dir" => ?container_image_depot_dir.as_ref(),
            "cache_size" => ?cache_size,
            "inline_limit" => ?inline_limit,
            "slots" => ?slots,
        );
        let msg = proto::StartRequest {
            broker_addr: broker_addr.into_proto_buf(),
            project_dir: project_dir.as_ref().into_proto_buf(),
            state_dir: state_dir.as_ref().into_proto_buf(),
            cache_dir: cache_dir.as_ref().into_proto_buf(),
            container_image_depot_dir: container_image_depot_dir.as_ref().into_proto_buf(),
            cache_size: cache_size.into_proto_buf(),
            inline_limit: inline_limit.into_proto_buf(),
            slots: slots.into_proto_buf(),
        };
        s.send_sync(|mut client| async move { client.start(msg).await })?;
        slog::debug!(s.log, "client completed start");

        Ok(s)
    }

    fn send_async<BuilderT, FutureT, ProtRetT>(
        &self,
        builder: BuilderT,
    ) -> Result<std::sync::mpsc::Receiver<Result<ProtRetT::Output>>>
    where
        BuilderT: FnOnce(ClientProcessClient<tonic::transport::Channel>) -> FutureT,
        BuilderT: Send + Sync + 'static,
        FutureT:
            Future<Output = std::result::Result<tonic::Response<ProtRetT>, tonic::Status>> + Send,
        ProtRetT: IntoResult,
        ProtRetT::Output: Send + 'static,
    {
        let (send, recv) = std::sync::mpsc::channel();
        self.requester
            .as_ref()
            .unwrap()
            .send(Box::new(move |client| {
                Box::pin(async move {
                    let _ = send.send(flatten_rpc_result(builder(client).await));
                })
            }))
            .with_context(|| "sending RPC request to client process")?;
        Ok(recv)
    }

    fn send_sync<BuilderT, FutureT, ProtRetT>(&self, builder: BuilderT) -> Result<ProtRetT::Output>
    where
        BuilderT: FnOnce(ClientProcessClient<tonic::transport::Channel>) -> FutureT,
        BuilderT: Send + Sync + 'static,
        FutureT:
            Future<Output = std::result::Result<tonic::Response<ProtRetT>, tonic::Status>> + Send,
        ProtRetT: IntoResult,
        ProtRetT::Output: Send + 'static,
    {
        self.send_async(builder)?
            .recv()
            .with_context(|| "receiving RPC response from client process")?
    }

    pub fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        slog::debug!(self.log, "client.add_layer"; "layer" => ?layer);
        let msg = proto::AddLayerRequest {
            layer: Some(layer.clone().into_proto_buf()),
        };
        let spec = self
            .send_sync(move |mut client| async move { client.add_layer(msg).await })
            .with_context(|| format!("adding layer {layer:#?}"))?;
        slog::debug!(self.log, "client.add_layer complete");
        Ok((
            TryFromProtoBuf::try_from_proto_buf(spec.digest)?,
            TryFromProtoBuf::try_from_proto_buf(spec.r#type)?,
        ))
    }

    pub fn add_job(
        &self,
        spec: JobSpec,
        handler: impl FnOnce(Result<(ClientJobId, JobOutcomeResult)>) + Send + Sync + 'static,
    ) -> Result<()> {
        let msg = proto::RunJobRequest {
            spec: Some(spec.clone().into_proto_buf()),
        };
        self.requester
            .as_ref()
            .unwrap()
            .send(Box::new(move |mut client| {
                Box::pin(async move {
                    let res = async move {
                        let res = client.run_job(msg).await?.into_inner();
                        let result: proto::JobOutcomeResult = res
                            .result
                            .ok_or_else(|| anyhow!("malformed RunJobResponse"))
                            .with_context(|| format!("adding job {spec:#?}"))?;
                        Result::<_, anyhow::Error>::Ok((
                            TryFromProtoBuf::try_from_proto_buf(res.client_job_id)?,
                            TryFromProtoBuf::try_from_proto_buf(result)?,
                        ))
                    }
                    .await;
                    tokio::task::spawn_blocking(move || handler(res));
                })
            }))?;
        Ok(())
    }

    pub fn introspect(&self) -> Result<IntrospectResponse> {
        self.send_sync(move |mut client| async move {
            let res = client.introspect(proto::Void {}).await?;
            Ok(res.map(TryFromProtoBuf::try_from_proto_buf))
        })
    }
}

pub fn bg_proc_main() -> Result<()> {
    let name = format!("maelstrom-client-{}", std::process::id());
    maelstrom_client_process::clone_into_pid_and_user_namespace()?;

    maelstrom_util::log::run_with_logger(maelstrom_util::config::common::LogLevel::Debug, |log| {
        let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name.as_bytes())?)?;
        slog::info!(log, "listening on unix-abstract:{name}");

        println!("{name}");

        let (sock, addr) = listener.accept()?;
        slog::info!(log, "got connection"; "address" => ?addr);

        let res = maelstrom_client_process::main_after_clone(
            sock,
            LoggerFactory::FromLogger(log.clone()),
        );
        slog::info!(log, "shutting down"; "res" => ?res);
        res
    })
}
