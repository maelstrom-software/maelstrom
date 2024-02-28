pub mod test;

pub use maelstrom_client_base::{
    spec, ArtifactUploadProgress, ClientDriverMode, ClientMessageKind, JobResponseHandler,
    MANIFEST_DIR,
};

use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use maelstrom_base::{stats::JobStateCounts, ArtifactType, JobSpec, Sha256Digest};
use maelstrom_client_base::{
    proto::{self, client_process_client::ClientProcessClient},
    IntoProtoBuf, IntoResult, TryFromProtoBuf,
};
use maelstrom_container::ContainerImage;
use maelstrom_util::config::BrokerAddr;
use spec::Layer;
use std::future::Future;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::pin::Pin;
use std::thread;
use tokio_stream::StreamExt as _;

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
            maelstrom_client_process::run_process_client(sock2).unwrap();
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
    log: slog::Logger,
}

fn flatten_rpc_result<ProtRetT>(res: TonicResponse<ProtRetT>) -> Result<ProtRetT::Output>
where
    ProtRetT: IntoResult,
{
    res?.into_inner().into_result()
}

async fn run_progress_bar(
    prog: ProgressBar,
    stream: tonic::Response<tonic::Streaming<proto::GetContainerImageProgressResponse>>,
) -> TonicResponse<proto::GetContainerImageResponse> {
    use proto::get_container_image_progress_response::Progress;
    let (meta, mut stream, ext) = stream.into_parts();
    while let Some(msg) = stream.next().await {
        match msg?.progress {
            Some(Progress::ProgressLength(len)) => prog.set_length(len),
            Some(Progress::ProgressInc(v)) => prog.inc(v),
            Some(Progress::Done(v)) => {
                return Ok(tonic::Response::from_parts(meta, v, ext));
            }
            None => break,
        }
    }
    Err(tonic::Status::new(
        tonic::Code::Unknown,
        "malformed progress response",
    ))
}

impl Client {
    pub fn new(
        mut process_handle: ClientBgProcess,
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
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

        slog::debug!(s.log, "client sending start";
            "driver_mode" => ?driver_mode,
            "broker_addr" => ?broker_addr,
            "project_dir" => ?project_dir.as_ref(),
            "cache_dir" => ?cache_dir.as_ref(),
        );
        let msg = proto::StartRequest {
            driver_mode: driver_mode.into_proto_buf(),
            broker_addr: broker_addr.into_proto_buf(),
            project_dir: project_dir.as_ref().into_proto_buf(),
            cache_dir: cache_dir.as_ref().into_proto_buf(),
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
            }))?;
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
        self.send_async(builder)?.recv()?
    }

    pub fn add_artifact(&self, path: &Path) -> Result<Sha256Digest> {
        slog::debug!(self.log, "client.add_artifact"; "path" => ?path);
        let msg = proto::AddArtifactRequest {
            path: path.into_proto_buf(),
        };
        let digest =
            self.send_sync(move |mut client| async move { client.add_artifact(msg).await })?;
        slog::debug!(self.log, "client.add_artifact complete");
        Ok(digest.try_into()?)
    }

    pub fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        slog::debug!(self.log, "client.add_layer"; "layer" => ?layer);
        let msg = proto::AddLayerRequest {
            layer: Some(layer.into_proto_buf()),
        };
        let spec = self.send_sync(move |mut client| async move { client.add_layer(msg).await })?;
        slog::debug!(self.log, "client.add_layer complete");
        Ok((
            TryFromProtoBuf::try_from_proto_buf(spec.digest)?,
            TryFromProtoBuf::try_from_proto_buf(spec.r#type)?,
        ))
    }

    pub fn get_container_image(
        &self,
        name: &str,
        tag: &str,
        prog: ProgressBar,
    ) -> Result<ContainerImage> {
        let msg = proto::GetContainerImageRequest {
            name: name.into(),
            tag: tag.into(),
        };
        let img = self.send_sync(move |mut client| async move {
            run_progress_bar(prog, client.get_container_image(msg).await?).await
        })?;
        TryFromProtoBuf::try_from_proto_buf(img)
    }

    pub fn add_job(&self, spec: JobSpec, handler: JobResponseHandler) -> Result<()> {
        let msg = proto::AddJobRequest {
            spec: Some(spec.into_proto_buf()),
        };
        self.requester
            .as_ref()
            .unwrap()
            .send(Box::new(move |mut client| {
                Box::pin(async move {
                    let inner = async move {
                        let res = client.add_job(msg).await?.into_inner();
                        let result: proto::JobOutcomeResult =
                            res.result.ok_or(anyhow!("malformed AddJobResponse"))?;
                        Result::<_, anyhow::Error>::Ok((
                            TryFromProtoBuf::try_from_proto_buf(res.client_job_id)?,
                            TryFromProtoBuf::try_from_proto_buf(result)?,
                        ))
                    };
                    if let Ok((cjid, result)) = inner.await {
                        tokio::task::spawn_blocking(move || handler(cjid, result));
                    }
                })
            }))?;
        Ok(())
    }

    pub fn stop_accepting(&self) -> Result<()> {
        self.send_sync(
            move |mut client| async move { client.stop_accepting(proto::Void {}).await },
        )?;
        Ok(())
    }

    pub fn wait_for_outstanding_jobs(&self) -> Result<()> {
        self.send_sync(move |mut client| async move {
            client.wait_for_outstanding_jobs(proto::Void {}).await
        })?;
        Ok(())
    }

    pub fn get_job_state_counts(
        &self,
    ) -> Result<std::sync::mpsc::Receiver<Result<JobStateCounts>>> {
        self.send_async(move |mut client| async move {
            let res = client.get_job_state_counts(proto::Void {}).await?;
            Ok(res.map(|v| TryFromProtoBuf::try_from_proto_buf(v.into_result()?)))
        })
    }

    pub fn get_artifact_upload_progress(&self) -> Result<Vec<ArtifactUploadProgress>> {
        self.send_sync(move |mut client| async move {
            let res = client.get_artifact_upload_progress(proto::Void {}).await?;
            Ok(res.map(|v| TryFromProtoBuf::try_from_proto_buf(v.into_result()?)))
        })
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_broker_msg_single_threaded(&self, count: usize) {
        slog::debug!(self.log, "client.process_broker_msg_single_threaded"; "count" => count);
        self.send_sync(move |mut client| async move {
            client
                .process_broker_msg_single_threaded(proto::ProcessBrokerMsgSingleThreadedRequest {
                    count: count as u64,
                })
                .await
        })
        .unwrap();
        slog::debug!(
            self.log,
            "client.process_broker_msg_single_threaded complete"
        );
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_client_messages_single_threaded(&self, kind: ClientMessageKind) {
        slog::debug!(
            self.log, "client.process_client_messages_single_threaded";
            "kind" => ?kind
        );
        self.send_sync(move |mut client| async move {
            client
                .process_client_messages_single_threaded(
                    proto::ProcessClientMessagesSingleThreadedRequest {
                        kind: kind.into_proto_buf(),
                    },
                )
                .await
        })
        .unwrap();
        slog::debug!(
            self.log,
            "client.process_client_messages_single_threaded complete"
        );
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    pub fn process_artifact_single_threaded(&self) {
        slog::debug!(self.log, "client.process_artifact_single_threaded");
        self.send_sync(move |mut client| async move {
            client
                .process_artifact_single_threaded(proto::Void {})
                .await
        })
        .unwrap();
        slog::debug!(self.log, "client.process_artifact_single_threaded complete");
    }
}
