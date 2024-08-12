use crate::client::Client;
use anyhow::Result;
use maelstrom_client_base::{
    proto::{self, client_process_server::ClientProcess},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, IntoProtoBuf, IntoResult, ProjectDir, StateDir,
    TryFromProtoBuf,
};
use maelstrom_container::ContainerImageDepotDir;
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    root::RootBuf,
};
use std::{result, sync::Arc};
use tonic::{Code, Request, Response, Status};

type TonicResult<T> = result::Result<T, Status>;
type TonicResponse<T> = TonicResult<Response<T>>;

pub struct Handler {
    client: Arc<Client>,
}

impl Handler {
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

trait ResultExt<T> {
    fn map_to_tonic(self) -> TonicResponse<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn map_to_tonic(self) -> TonicResponse<T> {
        match self {
            Ok(v) => Ok(Response::new(v)),
            Err(e) => Err(Status::new(Code::Unknown, format!("{e:?}"))),
        }
    }
}

#[allow(clippy::unit_arg)]
#[tonic::async_trait]
impl ClientProcess for Handler {
    async fn start(&self, request: Request<proto::StartRequest>) -> TonicResponse<proto::Void> {
        async {
            let request = request.into_inner();
            self.client
                .start(
                    Option::<BrokerAddr>::try_from_proto_buf(request.broker_addr)?,
                    RootBuf::<ProjectDir>::try_from_proto_buf(request.project_dir)?,
                    RootBuf::<StateDir>::try_from_proto_buf(request.state_dir)?,
                    RootBuf::<CacheDir>::try_from_proto_buf(request.cache_dir)?,
                    RootBuf::<ContainerImageDepotDir>::try_from_proto_buf(
                        request.container_image_depot_dir,
                    )?,
                    CacheSize::try_from_proto_buf(request.cache_size)?,
                    InlineLimit::try_from_proto_buf(request.inline_limit)?,
                    Slots::try_from_proto_buf(request.slots)?,
                    AcceptInvalidRemoteContainerTlsCerts::try_from_proto_buf(
                        request.accept_invalid_remote_container_tls_certs,
                    )?,
                )
                .await
                .map(IntoProtoBuf::into_proto_buf)
        }
        .await
        .map_to_tonic()
    }

    async fn run_job(
        &self,
        request: Request<proto::RunJobRequest>,
    ) -> TonicResponse<proto::RunJobResponse> {
        async {
            let spec = request.into_inner().into_result()?;
            let spec = TryFromProtoBuf::try_from_proto_buf(spec)?;
            self.client
                .run_job(spec)
                .await
                .map(|(cjid, res)| proto::RunJobResponse {
                    client_job_id: cjid.into_proto_buf(),
                    result: Some(res.into_proto_buf()),
                })
        }
        .await
        .map_to_tonic()
    }

    async fn introspect(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<proto::IntrospectResponse> {
        self.client
            .introspect()
            .await
            .map(|res| res.into_proto_buf())
            .map_to_tonic()
    }
}
