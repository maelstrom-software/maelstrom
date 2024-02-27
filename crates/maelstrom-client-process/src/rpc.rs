use crate::Client as ProcessClient;
use anyhow::{anyhow, Result};
use futures::stream::StreamExt as _;
use maelstrom_client_base::{proto, IntoProtoBuf, IntoResult, TryFromProtoBuf};
use maelstrom_container::ProgressTracker;
use std::future::Future;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream};

type TonicResult<T> = std::result::Result<T, tonic::Status>;
type TonicResponse<T> = TonicResult<tonic::Response<T>>;

#[derive(Clone)]
struct Handler {
    client: Arc<Mutex<Option<ProcessClient>>>,
}

impl Handler {
    fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
        }
    }

    async fn take_client(&self) -> Result<ProcessClient> {
        let mut guard = self.client.lock().await;
        guard.take().ok_or(anyhow!("must call start first"))
    }
}

macro_rules! with_client_async {
    ($s:expr, |$client:ident| { $($body:expr);* }) => {
        async {
            let mut client_guard = $s.client.lock().await;
            let $client = client_guard
                .as_mut()
                .ok_or(anyhow!("must call start first"))?;
            let res: Result::<_, anyhow::Error> = { $($body);* };
            res
        }
    };
}

async fn run_handler<RetT>(body: impl Future<Output = Result<RetT>>) -> TonicResponse<RetT> {
    match body.await {
        Ok(v) => Ok(tonic::Response::new(v)),
        Err(e) => Err(tonic::Status::new(tonic::Code::Unknown, e.to_string())),
    }
}

async fn map_err<RetT>(body: impl Future<Output = Result<RetT>>) -> TonicResult<RetT> {
    body.await
        .map_err(|e| tonic::Status::new(tonic::Code::Unknown, e.to_string()))
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
impl proto::client_process_server::ClientProcess for Handler {
    type GetContainerImageStream =
        Pin<Box<dyn Stream<Item = TonicResult<proto::GetContainerImageProgressResponse>> + Send>>;

    async fn start(
        &self,
        request: tonic::Request<proto::StartRequest>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let request = request.into_inner();
            let client = ProcessClient::new(
                TryFromProtoBuf::try_from_proto_buf(request.driver_mode)?,
                TryFromProtoBuf::try_from_proto_buf(request.broker_addr)?,
                PathBuf::try_from_proto_buf(request.project_dir)?,
                PathBuf::try_from_proto_buf(request.cache_dir)?,
            )
            .await?;
            *self.client.lock().await = Some(client);
            Ok(proto::Void {})
        })
        .await
    }

    async fn add_artifact(
        &self,
        request: tonic::Request<proto::AddArtifactRequest>,
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
        request: tonic::Request<proto::AddLayerRequest>,
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
        request: tonic::Request<proto::GetContainerImageRequest>,
    ) -> TonicResponse<Self::GetContainerImageStream> {
        run_handler(async {
            let (sender, receiver) = mpsc::unbounded_channel();
            let tracker = ChannelProgressTracker::new(sender);
            let proto::GetContainerImageRequest { name, tag } = request.into_inner();

            let self_clone = self.clone();
            tokio::task::spawn(async move {
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

    async fn stop_accepting(
        &self,
        _request: tonic::Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            with_client_async!(self, |client| { client.stop_accepting().await }).await?;
            Ok(proto::Void {})
        })
        .await
    }

    async fn add_job(
        &self,
        request: tonic::Request<proto::AddJobRequest>,
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
            let (cjid, res) = recv.recv().await.ok_or(anyhow!("client shutdown"))?;
            Ok(proto::AddJobResponse {
                client_job_id: cjid.into_proto_buf(),
                result: Some(res.into_proto_buf()),
            })
        })
        .await
    }

    async fn wait_for_outstanding_jobs(
        &self,
        _request: tonic::Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let mut client = self.take_client().await?;
            client.wait_for_outstanding_jobs().await?;
            Ok(proto::Void {})
        })
        .await
    }

    async fn get_job_state_counts(
        &self,
        _request: tonic::Request<proto::Void>,
    ) -> TonicResponse<proto::GetJobStateCountsResponse> {
        run_handler(async {
            let res = with_client_async!(self, |client| { client.get_job_state_counts().await })
                .await?
                .recv()
                .await
                .ok_or(anyhow!("client shutdown"))?;
            Ok(proto::GetJobStateCountsResponse {
                counts: Some(res.into_proto_buf()),
            })
        })
        .await
    }

    async fn get_artifact_upload_progress(
        &self,
        _request: tonic::Request<proto::Void>,
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
        request: tonic::Request<proto::ProcessBrokerMsgSingleThreadedRequest>,
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
        request: tonic::Request<proto::ProcessClientMessagesSingleThreadedRequest>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let wanted = TryFromProtoBuf::try_from_proto_buf(request.into_inner().kind)?;
            loop {
                let kind = with_client_async!(self, |client| {
                    Ok(client.process_client_messages_single_threaded().await)
                })
                .await?;
                if kind.is_some_and(|k| k == wanted) {
                    break;
                }
            }
            Ok(proto::Void {})
        })
        .await
    }

    async fn process_artifact_single_threaded(
        &self,
        _request: tonic::Request<proto::Void>,
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

type TokioError<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
pub async fn run_process_client(sock: UnixStream) -> Result<()> {
    sock.set_nonblocking(true)?;
    let sock1 = tokio::net::UnixStream::from_std(sock.try_clone()?)?;
    let sock2 = tokio::net::UnixStream::from_std(sock)?;
    tonic::transport::Server::builder()
        .add_service(proto::client_process_server::ClientProcessServer::new(
            Handler::new(),
        ))
        .serve_with_incoming_shutdown(
            tokio_stream::once(TokioError::<_>::Ok(sock1)).chain(tokio_stream::pending()),
            async move {
                loop {
                    let Ok(v) = sock2.ready(tokio::io::Interest::READABLE).await else {
                        continue;
                    };
                    if v.is_read_closed() {
                        break;
                    }
                    tokio::task::yield_now().await
                }
            },
        )
        .await?;
    Ok(())
}
