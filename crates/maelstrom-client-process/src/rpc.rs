use crate::Client;
use anyhow::{anyhow, Result};
use futures::stream::StreamExt as _;
use maelstrom_client_base::{
    proto::{
        self,
        client_process_server::{ClientProcess, ClientProcessServer},
    },
    IntoProtoBuf, IntoResult, TryFromProtoBuf,
};
use maelstrom_container::ProgressTracker;
use std::{
    error, future::Future, os::unix::net::UnixStream as StdUnixStream, path::PathBuf, pin::Pin,
    result, sync::Arc,
};
use tokio::{
    io::Interest,
    net::UnixStream as TokioUnixStream,
    sync::{mpsc, RwLock},
    task,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream};
use tonic::{transport::Server, Code, Request, Response, Status};

type TonicResult<T> = result::Result<T, Status>;
type TonicResponse<T> = TonicResult<Response<T>>;

#[derive(Clone)]
struct Handler {
    client: Arc<RwLock<Option<Client>>>,
    log: Option<slog::Logger>,
}

impl Handler {
    fn new(log: Option<slog::Logger>) -> Self {
        Self {
            client: Arc::new(RwLock::new(None)),
            log,
        }
    }

    async fn take_client(&self) -> Result<Client> {
        let mut guard = self.client.write().await;
        guard.take().ok_or_else(|| anyhow!("must call start first"))
    }
}

macro_rules! with_client_async {
    ($s:expr, |$client:ident| { $($body:expr);* }) => {
        async {
            let client_guard = $s.client.read().await;
            let $client = client_guard
                .as_ref()
                .ok_or_else(|| anyhow!("must call start first"))?;
            let res: Result<_> = { $($body);* };
            res
        }
    };
}

async fn run_handler<RetT>(body: impl Future<Output = Result<RetT>>) -> TonicResponse<RetT> {
    match body.await {
        Ok(v) => Ok(Response::new(v)),
        Err(e) => Err(Status::new(Code::Unknown, e.to_string())),
    }
}

async fn map_err<RetT>(body: impl Future<Output = Result<RetT>>) -> TonicResult<RetT> {
    body.await
        .map_err(|e| Status::new(Code::Unknown, e.to_string()))
}

#[derive(Clone)]
struct ChannelProgressTracker {
    sender: mpsc::UnboundedSender<TonicResult<proto::GetContainerImageProgressResponse>>,
}

impl ChannelProgressTracker {
    fn new(
        sender: mpsc::UnboundedSender<TonicResult<proto::GetContainerImageProgressResponse>>,
    ) -> Self {
        Self { sender }
    }

    fn finish(self, resp: TonicResult<proto::GetContainerImageResponse>) {
        use proto::get_container_image_progress_response::Progress;
        let _ = self
            .sender
            .send(resp.map(|v| proto::GetContainerImageProgressResponse {
                progress: Some(Progress::Done(v)),
            }));
    }
}

impl ProgressTracker for ChannelProgressTracker {
    fn set_length(&self, length: u64) {
        use proto::get_container_image_progress_response::Progress;
        let _ = self
            .sender
            .send(Ok(proto::GetContainerImageProgressResponse {
                progress: Some(Progress::ProgressLength(length)),
            }));
    }

    fn inc(&self, v: u64) {
        use proto::get_container_image_progress_response::Progress;
        let _ = self
            .sender
            .send(Ok(proto::GetContainerImageProgressResponse {
                progress: Some(Progress::ProgressInc(v)),
            }));
    }
}

#[allow(clippy::unit_arg)]
#[tonic::async_trait]
impl ClientProcess for Handler {
    type GetContainerImageStream =
        Pin<Box<dyn Stream<Item = TonicResult<proto::GetContainerImageProgressResponse>> + Send>>;

    async fn start(&self, request: Request<proto::StartRequest>) -> TonicResponse<proto::Void> {
        run_handler(async {
            let request = request.into_inner();
            let client = Client::new(
                TryFromProtoBuf::try_from_proto_buf(request.driver_mode)?,
                TryFromProtoBuf::try_from_proto_buf(request.broker_addr)?,
                PathBuf::try_from_proto_buf(request.project_dir)?,
                PathBuf::try_from_proto_buf(request.cache_dir)?,
                self.log.clone(),
            )
            .await?;
            *self.client.write().await = Some(client);
            Ok(proto::Void {})
        })
        .await
    }

    async fn add_artifact(
        &self,
        request: Request<proto::AddArtifactRequest>,
    ) -> TonicResponse<proto::AddArtifactResponse> {
        run_handler(async {
            let request = request.into_inner();
            let path = PathBuf::try_from_proto_buf(request.path)?;
            let digest =
                with_client_async!(self, |client| { client.add_artifact(&path).await }).await?;
            Ok(proto::AddArtifactResponse {
                digest: digest.into_proto_buf(),
            })
        })
        .await
    }

    async fn add_layer(
        &self,
        request: Request<proto::AddLayerRequest>,
    ) -> TonicResponse<proto::AddLayerResponse> {
        run_handler(async {
            let layer = request.into_inner().into_result()?;
            let layer = TryFromProtoBuf::try_from_proto_buf(layer)?;
            let spec = with_client_async!(self, |client| { client.add_layer(layer).await }).await?;
            Ok(proto::AddLayerResponse {
                spec: Some(spec.into_proto_buf()),
            })
        })
        .await
    }

    async fn get_container_image(
        &self,
        request: Request<proto::GetContainerImageRequest>,
    ) -> TonicResponse<Self::GetContainerImageStream> {
        run_handler(async {
            let (sender, receiver) = mpsc::unbounded_channel();
            let tracker = ChannelProgressTracker::new(sender);
            let proto::GetContainerImageRequest { name, tag } = request.into_inner();

            let self_clone = self.clone();
            task::spawn(async move {
                let result = map_err(async {
                    let image = with_client_async!(self_clone, |client| {
                        client
                            .get_container_image(&name, &tag, tracker.clone())
                            .await
                    })
                    .await?;
                    Ok(proto::GetContainerImageResponse {
                        image: Some(image.into_proto_buf()),
                    })
                })
                .await;
                tracker.finish(result);
            });
            Ok(Box::pin(UnboundedReceiverStream::new(receiver)) as Self::GetContainerImageStream)
        })
        .await
    }

    async fn stop_accepting(&self, _request: Request<proto::Void>) -> TonicResponse<proto::Void> {
        run_handler(async {
            with_client_async!(self, |client| { client.stop_accepting().await }).await?;
            Ok(proto::Void {})
        })
        .await
    }

    async fn add_job(
        &self,
        request: Request<proto::AddJobRequest>,
    ) -> TonicResponse<proto::AddJobResponse> {
        run_handler(async {
            let spec = TryFromProtoBuf::try_from_proto_buf(request.into_inner().into_result()?)?;
            let (send, mut recv) = mpsc::unbounded_channel();
            with_client_async!(self, |client| {
                Ok(client
                    .add_job(
                        spec,
                        Box::new(move |cjid, res| {
                            let _ = send.send((cjid, res));
                        }),
                    )
                    .await)
            })
            .await?;
            let (cjid, res) = recv
                .recv()
                .await
                .ok_or_else(|| anyhow!("client shutdown"))?;
            Ok(proto::AddJobResponse {
                client_job_id: cjid.into_proto_buf(),
                result: Some(res.into_proto_buf()),
            })
        })
        .await
    }

    async fn wait_for_outstanding_jobs(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let client = self.take_client().await?;
            client.wait_for_outstanding_jobs().await?;
            Ok(proto::Void {})
        })
        .await
    }

    async fn get_job_state_counts(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::GetJobStateCountsResponse> {
        run_handler(async {
            let res =
                with_client_async!(self, |client| { client.get_job_state_counts().await }).await?;
            Ok(proto::GetJobStateCountsResponse {
                counts: Some(res.into_proto_buf()),
            })
        })
        .await
    }

    async fn get_artifact_upload_progress(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::GetArtifactUploadProgressResponse> {
        run_handler(async {
            let res = with_client_async!(self, |client| {
                Ok(client.get_artifact_upload_progress().await)
            })
            .await?;
            Ok(proto::GetArtifactUploadProgressResponse {
                progress: res.into_proto_buf(),
            })
        })
        .await
    }

    async fn process_broker_msg_single_threaded(
        &self,
        request: Request<proto::ProcessBrokerMsgSingleThreadedRequest>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let request = request.into_inner();
            with_client_async!(self, |client| {
                Ok(client
                    .process_broker_msg_single_threaded(request.count as usize)
                    .await)
            })
            .await?;
            Ok(proto::Void {})
        })
        .await
    }

    async fn process_client_messages_single_threaded(
        &self,
        request: Request<proto::ProcessClientMessagesSingleThreadedRequest>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let wanted = TryFromProtoBuf::try_from_proto_buf(request.into_inner().kind)?;
            with_client_async!(self, |client| {
                Ok(client.process_client_messages_single_threaded(wanted).await)
            })
            .await?;
            Ok(proto::Void {})
        })
        .await
    }

    async fn process_artifact_single_threaded(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            with_client_async!(self, |client| {
                Ok(client.process_artifact_single_threaded().await)
            })
            .await?;
            Ok(proto::Void {})
        })
        .await
    }
}

type TokioError<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[tokio::main]
pub async fn client_process_main(sock: StdUnixStream, log: Option<slog::Logger>) -> Result<()> {
    sock.set_nonblocking(true)?;
    let sock1 = TokioUnixStream::from_std(sock.try_clone()?)?;
    let sock2 = TokioUnixStream::from_std(sock)?;
    Server::builder()
        .add_service(ClientProcessServer::new(Handler::new(log)))
        .serve_with_incoming_shutdown(
            tokio_stream::once(TokioError::<_>::Ok(sock1)).chain(tokio_stream::pending()),
            async move {
                loop {
                    let Ok(v) = sock2.ready(Interest::READABLE).await else {
                        continue;
                    };
                    if v.is_read_closed() {
                        break;
                    }
                    task::yield_now().await
                }
            },
        )
        .await?;
    Ok(())
}
