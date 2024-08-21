use crate::client::Client;
use crate::log::RpcLogSink;
use anyhow::{bail, Result};
use futures::{Stream, StreamExt as _};
use maelstrom_client_base::{
    proto::{self, client_process_server::ClientProcess},
    AddContainerRequest, IntoProtoBuf, RunJobRequest, StartRequest, TryFromProtoBuf,
};
use maelstrom_util::config::common::LogLevel;
use slog::Drain as _;
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
    rpc_log_level: LogLevel,
    log: Mutex<Option<slog::Logger>>,
}

impl Handler {
    pub fn new(client: Client, log: Option<slog::Logger>, rpc_log_level: LogLevel) -> Self {
        Self {
            client: Arc::new(client),
            rpc_log_level,
            log: Mutex::new(log),
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
        let drain = slog_async::Async::new(sink).build().fuse();
        let drain = slog::LevelFilter::new(drain, self.rpc_log_level.as_slog_level()).fuse();
        *self.log.lock().unwrap() = Some(slog::Logger::root(drain, slog::o!()));
        Ok(Box::pin(outgoing.map(|m| Ok(m.into_proto_buf()))) as Self::StreamLogMessagesStream)
            .map_to_tonic()
    }

    async fn start(&self, request: Request<proto::StartRequest>) -> TonicResponse<proto::Void> {
        async {
            let request: StartRequest = TryFromProtoBuf::try_from_proto_buf(request.into_inner())?;
            let Some(log) = ({ self.log.lock().unwrap().clone() }) else {
                bail!("no logging set up");
            };
            self.client
                .start(
                    log,
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

    type RunJobStream = Pin<Box<dyn Stream<Item = TonicResult<proto::JobStatus>> + Send>>;

    async fn run_job(
        &self,
        request: Request<proto::RunJobRequest>,
    ) -> TonicResponse<Self::RunJobStream> {
        async {
            let RunJobRequest { spec } = TryFromProtoBuf::try_from_proto_buf(request.into_inner())?;
            let stream = self.client.run_job(spec).await?;
            Ok(Box::pin(stream.map(|e| Ok(IntoProtoBuf::into_proto_buf(e)))) as Self::RunJobStream)
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
