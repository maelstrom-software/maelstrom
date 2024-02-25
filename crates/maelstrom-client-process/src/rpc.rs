use crate::Client as ProcessClient;
use anyhow::{anyhow, Result};
use futures::stream::StreamExt as _;
use maelstrom_client_base::{proto, IntoProtoBuf, IntoResult, TryFromProtoBuf};
use maelstrom_container::NullProgressTracker;
use std::future::Future;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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

    fn with_client<RetT>(
        &self,
        body: impl FnOnce(&mut ProcessClient) -> Result<RetT>,
    ) -> Result<RetT> {
        let mut guard = self.client.lock().unwrap();
        body(guard.as_mut().ok_or(anyhow!("must call start first"))?)
    }

    fn take_client(&self) -> Result<ProcessClient> {
        let mut guard = self.client.lock().unwrap();
        guard.take().ok_or(anyhow!("must call start first"))
    }
}

async fn run_handler<RetT>(body: impl Future<Output = Result<RetT>>) -> TonicResponse<RetT> {
    match body.await {
        Ok(v) => Ok(tonic::Response::new(v)),
        Err(e) => Err(tonic::Status::new(tonic::Code::Unknown, e.to_string())),
    }
}

#[tonic::async_trait]
impl proto::client_process_server::ClientProcess for Handler {
    async fn start(
        &self,
        request: tonic::Request<proto::StartRequest>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let request = request.into_inner();
            let client = tokio::task::spawn_blocking(move || {
                ProcessClient::new(
                    TryFromProtoBuf::try_from_proto_buf(request.driver_mode)?,
                    TryFromProtoBuf::try_from_proto_buf(request.broker_addr)?,
                    PathBuf::try_from_proto_buf(request.project_dir)?,
                    PathBuf::try_from_proto_buf(request.cache_dir)?,
                )
            })
            .await??;
            *self.client.lock().unwrap() = Some(client);
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
            let self_clone = self.clone();
            let digest = tokio::task::spawn_blocking(move || {
                self_clone.with_client(|client| {
                    client.add_artifact(&PathBuf::try_from_proto_buf(request.path)?)
                })
            })
            .await??;
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
            let self_clone = self.clone();
            let spec = tokio::task::spawn_blocking(move || {
                self_clone.with_client(|client| {
                    client.add_layer(TryFromProtoBuf::try_from_proto_buf(layer)?)
                })
            })
            .await??;
            Ok(proto::AddLayerResponse {
                spec: Some(spec.into_proto_buf()),
            })
        })
        .await
    }

    async fn get_container_image(
        &self,
        request: tonic::Request<proto::GetContainerImageRequest>,
    ) -> TonicResponse<proto::GetContainerImageResponse> {
        run_handler(async {
            let proto::GetContainerImageRequest { name, tag } = request.into_inner();
            let self_clone = self.clone();
            let image = tokio::task::spawn_blocking(move || {
                self_clone.with_client(|client| {
                    client.get_container_image(&name, &tag, NullProgressTracker)
                })
            })
            .await??;
            Ok(proto::GetContainerImageResponse {
                image: Some(image.into_proto_buf()),
            })
        })
        .await
    }

    async fn stop_accepting(
        &self,
        _request: tonic::Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            self.with_client(|client| client.stop_accepting())?;
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
            let (send, mut recv) = tokio::sync::mpsc::unbounded_channel();
            self.with_client(|client| {
                client.add_job(
                    spec,
                    Box::new(move |cjid, res| {
                        let _ = send.send((cjid, res));
                    }),
                );
                Ok(())
            })?;
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
            let mut client = self.take_client()?;
            tokio::task::spawn_blocking(move || client.wait_for_outstanding_jobs()).await??;
            Ok(proto::Void {})
        })
        .await
    }

    async fn get_job_state_counts(
        &self,
        _request: tonic::Request<proto::Void>,
    ) -> TonicResponse<proto::GetJobStateCountsResponse> {
        run_handler(async {
            let res = self
                .with_client(|client| client.get_job_state_counts())?
                .recv()
                .await
                .ok_or(anyhow!("client shutdown"))?;
            Ok(proto::GetJobStateCountsResponse {
                counts: Some(res.into_proto_buf()),
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
            let self_clone = self.clone();
            tokio::task::spawn_blocking(move || {
                self_clone.with_client(|client| {
                    client.process_broker_msg_single_threaded(request.count as usize);
                    Ok(())
                })
            })
            .await??;
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
            let self_clone = self.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                loop {
                    let kind = self_clone.with_client(|client| {
                        Ok(client.process_client_messages_single_threaded())
                    })?;
                    if kind.is_some_and(|k| k == wanted) {
                        break Ok(());
                    }
                }
            })
            .await??;
            Ok(proto::Void {})
        })
        .await
    }

    async fn process_artifact_single_threaded(
        &self,
        _request: tonic::Request<proto::Void>,
    ) -> TonicResponse<proto::Void> {
        run_handler(async {
            let self_clone = self.clone();
            tokio::task::spawn_blocking(move || {
                self_clone.with_client(|client| {
                    client.process_artifact_single_threaded();
                    Ok(())
                })
            })
            .await??;
            Ok(proto::Void {})
        })
        .await
    }
}

type TokioError<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
pub async fn run_process_client(sock: UnixStream) -> Result<()> {
    // XXX remi: There is some race going on I can't figure out yet
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

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
