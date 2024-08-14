use crate::client::Client;
use crate::log::RpcLogSink;
use anyhow::Result;
use futures::{Stream, StreamExt as _};
use maelstrom_client_base::{
    proto::{self, client_process_server::ClientProcess},
    spec::ContainerSpec,
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, IntoProtoBuf, IntoResult, ProjectDir, StateDir,
    TryFromProtoBuf,
};
use maelstrom_container::ContainerImageDepotDir;
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    root::RootBuf,
};
use std::pin::Pin;
use std::{
    result,
    sync::{Arc, Mutex},
};
use tonic::{Code, Request, Response, Status};

type TonicResult<T> = result::Result<T, Status>;
type TonicResponse<T> = TonicResult<Response<T>>;

pub struct Handler {
    client: Arc<Client>,
    log_sink: Mutex<Option<RpcLogSink>>,
}

impl Handler {
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
            log_sink: Default::default(),
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
    type StreamLogMessagesStream =
        Pin<Box<dyn Stream<Item = TonicResult<proto::LogMessage>> + Send>>;

    async fn stream_log_messages(
        &self,
        _request: Request<proto::Void>,
    ) -> TonicResponse<Self::StreamLogMessagesStream> {
        let (sink, outgoing) = RpcLogSink::new();
        *self.log_sink.lock().unwrap() = Some(sink);
        Ok(Box::pin(outgoing.map(|m| Ok(m.into_proto_buf()))) as Self::StreamLogMessagesStream)
            .map_to_tonic()
    }

    async fn start(&self, request: Request<proto::StartRequest>) -> TonicResponse<proto::Void> {
        async {
            let request = request.into_inner();
            let log_sink = { self.log_sink.lock().unwrap().clone() };
            self.client
                .start(
                    log_sink,
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

    async fn add_container(
        &self,
        request: Request<proto::AddContainerRequest>,
    ) -> TonicResponse<proto::Void> {
        async {
            let (name, container) = request.into_inner().into_result()?;
            self.client
                .add_container(
                    String::try_from_proto_buf(name)?,
                    ContainerSpec::try_from_proto_buf(container)?,
                )
                .await
                .map(IntoProtoBuf::into_proto_buf)
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
