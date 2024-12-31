//! This module contains a message queue that is backed by GitHub artifacts. It allows for
//! communication between jobs within the same workflow run.

use crate::{two_hours_from_now, Artifact, BackendIds, GitHubClient};
use anyhow::{anyhow, Result};
use azure_storage_blobs::prelude::BlobClient;
use futures::stream::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize)]
enum MessageHeader {
    KeepAlive,
    Payload { size: usize },
    Shutdown,
}

const READ_TIMEOUT: Duration = Duration::from_secs(10);

pub struct GitHubReadQueue {
    blob: BlobClient,
    index: usize,
    etag: Option<azure_core::Etag>,
    pending: VecDeque<Option<Vec<u8>>>,
}

impl GitHubReadQueue {
    async fn new(client: &GitHubClient, backend_ids: BackendIds, key: &str) -> Result<Self> {
        let blob = client.start_download(backend_ids, key).await?;
        Ok(Self {
            blob,
            index: 0,
            etag: None,
            pending: Default::default(),
        })
    }

    async fn maybe_read_msg(&mut self) -> Result<Option<Vec<u8>>> {
        let mut builder = self.blob.get().range(self.index..);

        if let Some(etag) = &self.etag {
            builder = builder.if_match(azure_core::request_options::IfMatchCondition::NotMatch(
                etag.to_string(),
            ));
        }

        let mut stream = builder.into_stream();
        let resp = stream
            .next()
            .await
            .ok_or_else(|| anyhow!("missing read response"))?;
        match resp {
            Ok(resp) => {
                self.etag = Some(resp.blob.properties.etag);

                let msg = resp.data.collect().await?;
                self.index += msg.len();
                Ok(Some(msg.to_vec()))
            }
            Err(err) => {
                use azure_core::{error::ErrorKind, StatusCode};

                match err.kind() {
                    ErrorKind::HttpResponse {
                        status: StatusCode::NotModified,
                        error_code: Some(error_code),
                    } if error_code == "ConditionNotMet" => {
                        return Ok(None);
                    }
                    ErrorKind::HttpResponse {
                        status: StatusCode::RequestedRangeNotSatisfiable,
                        error_code: Some(error_code),
                    } if error_code == "InvalidRange" => {
                        return Ok(None);
                    }
                    _ => {}
                }
                Err(err.into())
            }
        }
    }

    pub async fn read_msg(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(msg) = self.pending.pop_front() {
            return Ok(msg);
        }

        let mut read_start = Instant::now();
        loop {
            if let Some(res) = self.maybe_read_msg().await? {
                let mut r = &res[..];
                while !r.is_empty() {
                    let header: MessageHeader = bincode::deserialize_from(&mut r)?;
                    match header {
                        MessageHeader::KeepAlive => {
                            read_start = Instant::now();
                        }
                        MessageHeader::Payload { size } => {
                            let payload = r[..size].to_vec();
                            r = &r[size..];
                            self.pending.push_back(Some(payload));
                        }
                        MessageHeader::Shutdown => {
                            self.pending.push_back(None);
                        }
                    }
                }
            }

            if let Some(msg) = self.pending.pop_front() {
                return Ok(msg);
            }

            if read_start.elapsed() > READ_TIMEOUT {
                return Err(anyhow!("GitHub queue read timeout"));
            }
        }
    }
}

async fn send_keep_alive(blob: Arc<BlobClient>) {
    loop {
        tokio::time::sleep(READ_TIMEOUT / 2).await;
        let _ = blob
            .append_block(bincode::serialize(&MessageHeader::KeepAlive).unwrap())
            .await;
    }
}

pub struct GitHubWriteQueue {
    blob: Arc<BlobClient>,
    keep_alive: tokio::task::AbortHandle,
}

impl GitHubWriteQueue {
    async fn new(client: &GitHubClient, key: &str) -> Result<Self> {
        let blob = Arc::new(client.start_upload(key, Some(two_hours_from_now())).await?);
        blob.put_append_blob().await?;
        client.finish_upload(key, 0).await?;
        let keep_alive = tokio::task::spawn(send_keep_alive(blob.clone())).abort_handle();
        Ok(Self { blob, keep_alive })
    }

    pub async fn write_msg(&mut self, data: &[u8]) -> Result<()> {
        let mut to_send = bincode::serialize(&MessageHeader::Payload { size: data.len() }).unwrap();
        to_send.extend(data);
        self.blob.append_block(to_send).await?;

        self.keep_alive.abort();
        self.keep_alive = tokio::task::spawn(send_keep_alive(self.blob.clone())).abort_handle();

        Ok(())
    }

    pub async fn shut_down(&mut self) -> Result<()> {
        self.keep_alive.abort();
        self.blob
            .append_block(bincode::serialize(&MessageHeader::Shutdown).unwrap())
            .await?;
        Ok(())
    }
}

async fn wait_for_artifact(client: &GitHubClient, key: &str) -> Result<()> {
    while !client.list().await?.iter().any(|a| a.name == key) {}
    Ok(())
}

pub struct GitHubQueue {
    read: GitHubReadQueue,
    write: GitHubWriteQueue,
}

impl GitHubQueue {
    async fn new(
        client: &GitHubClient,
        read_backend_ids: BackendIds,
        read_key: &str,
        write_key: &str,
    ) -> Result<Self> {
        Ok(Self {
            write: GitHubWriteQueue::new(client, write_key).await?,
            read: GitHubReadQueue::new(client, read_backend_ids, read_key).await?,
        })
    }

    async fn maybe_connect(client: &GitHubClient, id: &str) -> Result<Option<Self>> {
        let artifacts = client.list().await?;
        if let Some(listener) = artifacts.iter().find(|a| a.name == format!("{id}-listen")) {
            let Artifact {
                name, backend_ids, ..
            } = listener;
            let key = name.strip_suffix("-listen").unwrap();
            let self_id = uuid::Uuid::new_v4().to_string();

            let write_key = format!("{self_id}-{key}-up");
            let write = GitHubWriteQueue::new(client, &write_key).await?;

            let read_key = format!("{self_id}-{key}-down");
            wait_for_artifact(client, &read_key).await?;
            let read = GitHubReadQueue::new(client, backend_ids.clone(), &read_key).await?;

            Ok(Some(Self { write, read }))
        } else {
            Ok(None)
        }
    }

    pub async fn connect(client: &GitHubClient, id: &str) -> Result<Self> {
        loop {
            if let Some(socket) = Self::maybe_connect(client, id).await? {
                return Ok(socket);
            }
        }
    }

    pub async fn read_msg(&mut self) -> Result<Option<Vec<u8>>> {
        self.read.read_msg().await
    }

    pub async fn write_msg(&mut self, data: &[u8]) -> Result<()> {
        self.write.write_msg(data).await
    }

    pub async fn shut_down(&mut self) -> Result<()> {
        self.write.shut_down().await
    }
}

pub struct GitHubQueueAcceptor {
    id: String,
    accepted: HashSet<String>,
    client: Arc<GitHubClient>,
}

impl GitHubQueueAcceptor {
    pub async fn new(client: impl Into<Arc<GitHubClient>>, id: &str) -> Result<Self> {
        let key = format!("{id}-listen");
        let client = client.into();
        client
            .upload(&key, Some(two_hours_from_now()), &[][..])
            .await?;
        Ok(Self {
            id: id.into(),
            accepted: HashSet::new(),
            client,
        })
    }

    async fn maybe_accept_one(&mut self) -> Result<Option<GitHubQueue>> {
        let artifacts = self.client.list().await?;
        if let Some(connected) = artifacts.iter().find(|a| {
            a.name.ends_with(&format!("{}-up", self.id)) && !self.accepted.contains(&a.name)
        }) {
            let Artifact {
                name, backend_ids, ..
            } = connected;
            let key = name.strip_suffix("-up").unwrap();
            let socket = GitHubQueue::new(
                &self.client,
                backend_ids.clone(),
                &format!("{key}-up"),
                &format!("{key}-down"),
            )
            .await?;
            self.accepted.insert(name.into());
            Ok(Some(socket))
        } else {
            Ok(None)
        }
    }

    pub async fn accept_one(&mut self) -> Result<GitHubQueue> {
        loop {
            if let Some(socket) = self.maybe_accept_one().await? {
                return Ok(socket);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn acceptor(client: GitHubClient) {
        let mut acceptor = GitHubQueueAcceptor::new(client, "foo").await.unwrap();

        let mut handles = vec![];
        for _ in 0..2 {
            let mut queue = acceptor.accept_one().await.unwrap();
            handles.push(tokio::task::spawn(async move {
                for _ in 0..3 {
                    queue.write_msg(&b"ping"[..]).await.unwrap();
                    let msg = queue.read_msg().await.unwrap().unwrap();
                    assert_eq!(msg, b"pong");
                }
                queue.shut_down().await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    async fn connector(client: GitHubClient) {
        let mut sock = GitHubQueue::connect(&client, "foo").await.unwrap();
        while let Some(msg) = sock.read_msg().await.unwrap() {
            assert_eq!(msg, b"ping");
            sock.write_msg(&b"pong"[..]).await.unwrap();
        }
    }

    #[tokio::test]
    async fn real_github_integration_test() {
        let Some(client) = crate::client::tests::client_factory() else {
            println!("skipping due to missing GitHub credentials");
            return;
        };
        println!("test found GitHub credentials");

        match &std::env::var("TEST_ACTOR").unwrap()[..] {
            "1" => acceptor(client).await,
            "2" => connector(client).await,
            "3" => connector(client).await,
            _ => panic!("unknown test actor"),
        }
    }
}
