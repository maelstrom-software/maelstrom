use crate::client::Client;
use crate::log::RpcLogSink;
use anyhow::Result;
use futures::{Stream, StreamExt as _};
use maelstrom_client_base::{
    proto::{self, client_process_server::ClientProcess},
    AddContainerRequest, IntoProtoBuf, RunJobRequest, RunJobResponse, StartRequest,
    TryFromProtoBuf,
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
            let request: StartRequest = TryFromProtoBuf::try_from_proto_buf(request.into_inner())?;
            let log_sink = { self.log_sink.lock().unwrap().clone() };
            self.client
                .start(
                    log_sink,
                    request.broker_addr,
                    request.project_dir,
                    request.state_dir,
                    request.cache_dir,
                    request.container_image_depot_dir,
                    request.cache_size,
                    request.inline_limit,
                    request.slots,
                    request.accept_invalid_remote_container_tls_certs,
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
            let RunJobRequest { spec } = TryFromProtoBuf::try_from_proto_buf(request.into_inner())?;
            self.client
                .run_job(spec)
                .await
                .map(|(client_job_id, result)| {
                    RunJobResponse {
                        client_job_id,
                        result,
                    }
                    .into_proto_buf()
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
            let AddContainerRequest { name, container } =
                TryFromProtoBuf::try_from_proto_buf(request.into_inner())?;
            self.client
                .add_container(name, container)
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
