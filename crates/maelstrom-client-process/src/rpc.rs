use crate::{
    client::{self, Client},
    stream_wrapper::StreamWrapper,
};
use anyhow::{anyhow, Result, Error};
use futures::stream::StreamExt as _;
use maelstrom_client_base::{
    proto::{
        self,
        client_process_server::{ClientProcess, ClientProcessServer},
    },
    IntoProtoBuf, IntoResult, TryFromProtoBuf,
};
use maelstrom_container::ProgressTracker;
use maelstrom_util::async_fs;
use slog::Logger;
use std::{
    error, os::unix::net::UnixStream as StdUnixStream, path::PathBuf, pin::Pin,
    result, sync::Arc,
};
use tokio::{
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
    log: Option<Logger>,
}

impl Handler {
    fn new(log: Option<Logger>) -> Self {
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

trait ResultExt<T> {
    fn map_to_tonic(self) -> TonicResponse<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn map_to_tonic(self) -> TonicResponse<T> {
        match self {
            Ok(v) => Ok(Response::new(v)),
            Err(e) => Err(Status::new(Code::Unknown, e.to_string())),
        }
    }
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
        async {
            let request = request.into_inner();
            let broker_addr = TryFromProtoBuf::try_from_proto_buf(request.broker_addr)?;
            let project_dir = PathBuf::try_from_proto_buf(request.project_dir)?;
            let cache_dir = PathBuf::try_from_proto_buf(request.cache_dir)?;
            let fs = async_fs::Fs::new();
            for d in [
                crate::MANIFEST_DIR,
                crate::STUB_MANIFEST_DIR,
                crate::SYMLINK_MANIFEST_DIR,
            ] {
                fs.create_dir_all(cache_dir.join(d)).await?;
            }
            let log = match &self.log {
                Some(log) => log.clone(),
                None => client::default_log(&fs, cache_dir.as_ref()).await?,
            };
            let (dispatcher_sender, upload_tracker) =
                client::start_tasks(broker_addr, log.clone()).await?;
            let client = Client::new(
                project_dir,
                cache_dir,
                dispatcher_sender,
                upload_tracker,
                log,
            )
            .await?;
            *self.client.write().await = Some(client);
            Ok(proto::Void {})
        }
        .await
        .map_to_tonic()
    }

    async fn add_artifact(
        &self,
        request: Request<proto::AddArtifactRequest>,
    ) -> TonicResponse<proto::AddArtifactResponse> {
        async {
            let request = request.into_inner();
            let path = PathBuf::try_from_proto_buf(request.path)?;
            Ok(proto::AddArtifactResponse {
                digest: with_client_async!(self, |client| { client.add_artifact(&path).await })
                    .await?
                    .into_proto_buf(),
            })
        }
        .await
        .map_to_tonic()
    }

    async fn add_layer(
        &self,
        request: Request<proto::AddLayerRequest>,
    ) -> TonicResponse<proto::AddLayerResponse> {
        async {
            let layer = request.into_inner().into_result()?;
            let layer = TryFromProtoBuf::try_from_proto_buf(layer)?;
            Ok(proto::AddLayerResponse {
                spec: Some(
                    with_client_async!(self, |client| { client.add_layer(layer).await })
                        .await?
                        .into_proto_buf(),
                ),
            })
        }
        .await
        .map_to_tonic()
    }

    async fn get_container_image(
        &self,
        request: Request<proto::GetContainerImageRequest>,
    ) -> TonicResponse<Self::GetContainerImageStream> {
        async {
            let (sender, receiver) = mpsc::unbounded_channel();
            let tracker = ChannelProgressTracker::new(sender);
            let proto::GetContainerImageRequest { name, tag } = request.into_inner();

            let self_clone = self.clone();
            task::spawn(async move {
                let result = async {
                    let image = with_client_async!(self_clone, |client| {
                        client
                            .get_container_image(&name, &tag, tracker.clone())
                            .await
                    })
                    .await?;
                    Ok(proto::GetContainerImageResponse {
                        image: Some(image.into_proto_buf()),
                    })
                }
                .await
                .map_err(|e: Error| Status::new(Code::Unknown, e.to_string()));
                tracker.finish(result);
            });
            Ok(Box::pin(UnboundedReceiverStream::new(receiver)) as Self::GetContainerImageStream)
        }
        .await
        .map_to_tonic()
    }

    async fn add_job(
        &self,
        request: Request<proto::AddJobRequest>,
    ) -> TonicResponse<proto::AddJobResponse> {
        async {
            let spec = TryFromProtoBuf::try_from_proto_buf(request.into_inner().into_result()?)?;
            with_client_async!(self, |client| { client.run_job(spec).await })
                .await
                .map(|(cjid, res)| proto::AddJobResponse {
                    client_job_id: cjid.into_proto_buf(),
                    result: Some(res.into_proto_buf()),
                })
        }
        .await
        .map_to_tonic()
    }

    async fn wait_for_outstanding_jobs(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        async {
            let client = self.take_client().await?;
            client.wait_for_outstanding_jobs().await?;
            Ok(proto::Void {})
        }
        .await
        .map_to_tonic()
    }

    async fn get_job_state_counts(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::GetJobStateCountsResponse> {
        async {
            Ok(proto::GetJobStateCountsResponse {
                counts: Some(
                    with_client_async!(self, |client| { client.get_job_state_counts().await })
                        .await?
                        .into_proto_buf(),
                ),
            })
        }
        .await
        .map_to_tonic()
    }

    async fn get_artifact_upload_progress(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::GetArtifactUploadProgressResponse> {
        async {
            Ok(proto::GetArtifactUploadProgressResponse {
                progress: with_client_async!(self, |client| {
                    Ok(client.get_artifact_upload_progress().await)
                })
                .await?
                .into_proto_buf(),
            })
        }
        .await
        .map_to_tonic()
    }
}

type TokioError<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[tokio::main]
pub async fn client_process_main(sock: StdUnixStream, log: Option<Logger>) -> Result<()> {
    sock.set_nonblocking(true)?;
    let (sock, receiver) = StreamWrapper::new(TokioUnixStream::from_std(sock)?);
    Server::builder()
        .add_service(ClientProcessServer::new(Handler::new(log)))
        .serve_with_incoming_shutdown(
            tokio_stream::once(TokioError::<_>::Ok(sock)).chain(tokio_stream::pending()),
            receiver,
        )
        .await?;
    Ok(())
}
