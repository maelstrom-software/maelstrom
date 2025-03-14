//! This module contains a message queue that is backed by GitHub artifacts. It allows for
//! communication between jobs within the same workflow run.

use crate::{two_hours_from_now, Artifact, BackendIds, GitHubClient};
use anyhow::{anyhow, Result};
use azure_core::Etag;
use azure_storage_blobs::prelude::BlobClient;
use futures::stream::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[allow(async_fn_in_trait)]
pub trait QueueConnection {
    type Blob: QueueBlob;

    async fn get_blob(&self, backend_ids: BackendIds, key: &str) -> Result<Self::Blob>;
    async fn create_blob(&self, key: &str) -> Result<Self::Blob>;
    async fn list(&self) -> Result<Vec<Artifact>>;
}

pub enum ReadResponse {
    Data { data: Vec<u8>, etag: Etag },
    NoData,
    AuthenticationFailed,
}

#[allow(async_fn_in_trait)]
pub trait QueueBlob: Send + Sync + 'static {
    async fn read(&self, index: usize, etag: &Option<Etag>) -> Result<ReadResponse>;
    fn write(&self, data: Vec<u8>) -> impl Future<Output = Result<()>> + Send;
}

impl QueueConnection for GitHubClient {
    type Blob = BlobClient;

    async fn get_blob(&self, backend_ids: BackendIds, key: &str) -> Result<Self::Blob> {
        self.start_download(backend_ids, key).await
    }

    async fn create_blob(&self, key: &str) -> Result<Self::Blob> {
        let blob = self.start_upload(key, Some(two_hours_from_now())).await?;
        blob.put_append_blob().await?;
        self.finish_upload(key, 0).await?;
        Ok(blob)
    }

    async fn list(&self) -> Result<Vec<Artifact>> {
        GitHubClient::list(self).await
    }
}

impl QueueBlob for BlobClient {
    async fn read(&self, index: usize, etag: &Option<Etag>) -> Result<ReadResponse> {
        let mut builder = self.get().range(index..);

        if let Some(etag) = etag {
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
                let msg = resp.data.collect().await?;
                Ok(ReadResponse::Data {
                    data: msg.to_vec(),
                    etag: resp.blob.properties.etag,
                })
            }
            Err(err) => {
                use azure_core::{error::ErrorKind, StatusCode};

                match err.kind() {
                    ErrorKind::HttpResponse {
                        status: StatusCode::NotModified,
                        error_code: Some(error_code),
                    } if error_code == "ConditionNotMet" => {
                        return Ok(ReadResponse::NoData);
                    }
                    ErrorKind::HttpResponse {
                        status: StatusCode::RequestedRangeNotSatisfiable,
                        error_code: Some(error_code),
                    } if error_code == "InvalidRange" => {
                        return Ok(ReadResponse::NoData);
                    }
                    ErrorKind::HttpResponse {
                        status: StatusCode::Forbidden,
                        error_code: Some(error_code),
                    } if error_code == "AuthenticationFailed" => {
                        return Ok(ReadResponse::AuthenticationFailed);
                    }
                    _ => {}
                }
                Err(err.into())
            }
        }
    }

    async fn write(&self, to_send: Vec<u8>) -> Result<()> {
        self.append_block(to_send).await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Copy, Clone, Debug)]
enum MessageHeader {
    KeepAlive,
    Payload { size: usize },
    Shutdown,
}

const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);

pub struct GitHubReadQueue<ConnT: QueueConnection = GitHubClient> {
    conn: Arc<ConnT>,
    blob: ConnT::Blob,
    index: usize,
    etag: Option<Etag>,
    pending: VecDeque<Option<Vec<u8>>>,
    read_timeout: Duration,
    backend_ids: BackendIds,
    key: String,
}

impl<ConnT> GitHubReadQueue<ConnT>
where
    ConnT: QueueConnection,
{
    async fn new(
        conn: Arc<ConnT>,
        read_timeout: Duration,
        backend_ids: BackendIds,
        key: &str,
    ) -> Result<Self> {
        let blob = conn.get_blob(backend_ids.clone(), key).await?;
        Ok(Self {
            conn,
            blob,
            index: 0,
            etag: None,
            pending: Default::default(),
            read_timeout,
            backend_ids,
            key: key.into(),
        })
    }

    async fn maybe_read_msg(&mut self) -> Result<Option<Vec<u8>>> {
        let (msg, etag) = match self.blob.read(self.index, &self.etag).await? {
            ReadResponse::Data { data, etag } => (data, etag),
            ReadResponse::NoData => return Ok(None),
            ReadResponse::AuthenticationFailed => {
                self.blob = self
                    .conn
                    .get_blob(self.backend_ids.clone(), &self.key)
                    .await?;
                return Ok(None);
            }
        };

        self.etag = Some(etag);
        self.index += msg.len();
        Ok(Some(msg))
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

            if read_start.elapsed() > self.read_timeout {
                return Err(anyhow!("GitHub queue read timeout"));
            }
        }
    }
}

async fn send_keep_alive(duration: Duration, blob: Arc<impl QueueBlob>) {
    loop {
        tokio::time::sleep(duration).await;
        let _ = blob
            .write(bincode::serialize(&MessageHeader::KeepAlive).unwrap())
            .await;
    }
}

pub struct GitHubWriteQueue<BlobT = BlobClient> {
    blob: Arc<BlobT>,
    keep_alive: tokio::task::AbortHandle,
    keep_alive_duration: Duration,
}

impl<BlobT: QueueBlob> GitHubWriteQueue<BlobT> {
    async fn new<ConnT>(conn: &ConnT, keep_alive_duration: Duration, key: &str) -> Result<Self>
    where
        ConnT: QueueConnection<Blob = BlobT>,
    {
        let blob = Arc::new(conn.create_blob(key).await?);
        let keep_alive =
            tokio::task::spawn(send_keep_alive(keep_alive_duration, blob.clone())).abort_handle();
        Ok(Self {
            blob,
            keep_alive,
            keep_alive_duration,
        })
    }

    pub async fn write_msg(&mut self, data: &[u8]) -> Result<()> {
        let mut to_send = bincode::serialize(&MessageHeader::Payload { size: data.len() }).unwrap();
        to_send.extend(data);
        self.blob.write(to_send).await?;

        self.keep_alive.abort();
        self.keep_alive =
            tokio::task::spawn(send_keep_alive(self.keep_alive_duration, self.blob.clone()))
                .abort_handle();

        Ok(())
    }

    pub async fn write_many_msgs(&mut self, messages: &[Vec<u8>]) -> Result<()> {
        let mut to_send = vec![];
        for data in messages {
            to_send
                .extend(bincode::serialize(&MessageHeader::Payload { size: data.len() }).unwrap());
            to_send.extend(data);
        }
        self.blob.write(to_send).await?;

        self.keep_alive.abort();
        self.keep_alive =
            tokio::task::spawn(send_keep_alive(self.keep_alive_duration, self.blob.clone()))
                .abort_handle();

        Ok(())
    }

    pub async fn shut_down(&mut self) -> Result<()> {
        self.keep_alive.abort();
        self.blob
            .write(bincode::serialize(&MessageHeader::Shutdown).unwrap())
            .await?;
        Ok(())
    }
}

impl<BlobT> Drop for GitHubWriteQueue<BlobT> {
    fn drop(&mut self) {
        self.keep_alive.abort();
    }
}

async fn wait_for_artifact(conn: &impl QueueConnection, key: &str) -> Result<()> {
    while !conn.list().await?.iter().any(|a| a.name == key) {}
    Ok(())
}

pub struct GitHubQueue<ConnT: QueueConnection = GitHubClient> {
    read: GitHubReadQueue<ConnT>,
    write: GitHubWriteQueue<ConnT::Blob>,
}

impl<ConnT> GitHubQueue<ConnT>
where
    ConnT: QueueConnection,
{
    async fn new(
        conn: Arc<ConnT>,
        read_timeout: Duration,
        read_backend_ids: BackendIds,
        read_key: &str,
        write_key: &str,
    ) -> Result<Self> {
        Ok(Self {
            write: GitHubWriteQueue::new(&*conn, read_timeout / 4, write_key).await?,
            read: GitHubReadQueue::new(conn, read_timeout, read_backend_ids, read_key).await?,
        })
    }

    async fn maybe_connect(conn: Arc<ConnT>, id: &str) -> Result<Option<Self>> {
        let artifacts = conn.list().await?;
        if let Some(listener) = artifacts.iter().find(|a| a.name == format!("{id}-listen")) {
            let Artifact {
                name, backend_ids, ..
            } = listener;
            let key = name.strip_suffix("-listen").unwrap();
            let self_id = uuid::Uuid::new_v4().to_string();

            let write_key = format!("{self_id}-{key}-up");
            let write = GitHubWriteQueue::new(&*conn, DEFAULT_READ_TIMEOUT / 4, &write_key).await?;

            let read_key = format!("{self_id}-{key}-down");
            wait_for_artifact(&*conn, &read_key).await?;
            let read =
                GitHubReadQueue::new(conn, DEFAULT_READ_TIMEOUT, backend_ids.clone(), &read_key)
                    .await?;

            Ok(Some(Self { write, read }))
        } else {
            Ok(None)
        }
    }

    pub async fn connect(conn: ConnT, id: &str) -> Result<Self> {
        let conn = Arc::new(conn);
        loop {
            if let Some(socket) = Self::maybe_connect(conn.clone(), id).await? {
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

    pub fn into_split(self) -> (GitHubReadQueue<ConnT>, GitHubWriteQueue<ConnT::Blob>) {
        (self.read, self.write)
    }
}

pub struct GitHubQueueAcceptor<ConnT = GitHubClient> {
    id: String,
    accepted: HashSet<String>,
    conn: Arc<ConnT>,
}

impl<ConnT> GitHubQueueAcceptor<ConnT>
where
    ConnT: QueueConnection,
{
    pub async fn new(conn: ConnT, id: &str) -> Result<Self> {
        let key = format!("{id}-listen");
        let conn = Arc::new(conn);
        conn.create_blob(&key).await?;
        Ok(Self {
            id: id.into(),
            accepted: HashSet::new(),
            conn,
        })
    }

    async fn maybe_accept_one(&mut self) -> Result<Option<GitHubQueue<ConnT>>> {
        let artifacts = self.conn.list().await?;
        if let Some(connected) = artifacts.iter().find(|a| {
            a.name.ends_with(&format!("{}-up", self.id)) && !self.accepted.contains(&a.name)
        }) {
            let Artifact {
                name, backend_ids, ..
            } = connected;
            let key = name.strip_suffix("-up").unwrap();
            let socket = GitHubQueue::new(
                self.conn.clone(),
                DEFAULT_READ_TIMEOUT,
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

    pub async fn accept_one(&mut self) -> Result<GitHubQueue<ConnT>> {
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
    use anyhow::bail;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    struct FakeConnection {
        blobs: Arc<Mutex<HashMap<String, FakeBlob>>>,
    }

    #[derive(Clone, Default)]
    struct FakeBlob {
        data: Arc<Mutex<Vec<u8>>>,
    }

    impl FakeBlob {
        fn len(&self) -> usize {
            self.data.lock().unwrap().len()
        }

        fn data(&self) -> Vec<u8> {
            self.data.lock().unwrap().clone()
        }
    }

    fn b_ids() -> BackendIds {
        BackendIds {
            workflow_run_backend_id: "b1".into(),
            workflow_job_run_backend_id: "b2".into(),
        }
    }

    impl QueueConnection for FakeConnection {
        type Blob = FakeBlob;

        async fn get_blob(&self, backend_ids: BackendIds, key: &str) -> Result<Self::Blob> {
            tokio::task::yield_now().await;

            assert_eq!(backend_ids, b_ids());
            Ok(self
                .blobs
                .lock()
                .unwrap()
                .get(key)
                .ok_or_else(|| anyhow!("blob not found"))?
                .clone())
        }

        async fn create_blob(&self, key: &str) -> Result<Self::Blob> {
            tokio::task::yield_now().await;

            let mut blobs = self.blobs.lock().unwrap();

            if blobs.contains_key(key) {
                bail!("blob already exists");
            }
            let new_blob = FakeBlob::default();
            blobs.insert(key.into(), new_blob.clone());
            Ok(new_blob)
        }

        async fn list(&self) -> Result<Vec<Artifact>> {
            tokio::task::yield_now().await;

            Ok(self
                .blobs
                .lock()
                .unwrap()
                .iter()
                .map(|(name, blob)| Artifact {
                    name: name.clone(),
                    backend_ids: b_ids(),
                    size: blob.len().try_into().unwrap(),
                    database_id: 1.into(),
                })
                .collect())
        }
    }

    impl QueueBlob for FakeBlob {
        async fn read(&self, index: usize, etag: &Option<Etag>) -> Result<ReadResponse> {
            use sha2::Digest as _;

            tokio::task::yield_now().await;

            let data = self.data.lock().unwrap();

            let mut hasher = sha2::Sha256::new();
            hasher.update(&data[..]);
            let actual_etag: Etag = maelstrom_base::Sha256Digest::new(hasher.finalize().into())
                .to_string()
                .into();

            if let Some(not_etag) = etag {
                if not_etag == &actual_etag {
                    return Ok(ReadResponse::NoData);
                }
            }

            if !data.is_empty() {
                assert!(index < data.len());
            }
            Ok(ReadResponse::Data {
                data: data[index..].to_vec(),
                etag: actual_etag,
            })
        }

        async fn write(&self, data: Vec<u8>) -> Result<()> {
            tokio::task::yield_now().await;

            self.data.lock().unwrap().extend(data);
            Ok(())
        }
    }

    const SHORT_DURATION: Duration = Duration::from_millis(100);
    const FOREVER: Duration = Duration::from_secs(u64::MAX);

    #[tokio::test]
    async fn read_single_msg() {
        let conn = FakeConnection::default();
        let b = conn.create_blob("foo").await.unwrap();
        let mut queue = GitHubReadQueue::new(Arc::new(conn), SHORT_DURATION, b_ids(), "foo")
            .await
            .unwrap();

        b.write(bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap())
            .await
            .unwrap();
        let sent_msg = vec![1, 2, 3, 4, 5];
        b.write(sent_msg.clone()).await.unwrap();

        let read_msg = queue.read_msg().await.unwrap().unwrap();
        assert_eq!(read_msg, sent_msg);
    }

    #[tokio::test]
    async fn read_multiple_msgs() {
        let conn = FakeConnection::default();
        let b = conn.create_blob("foo").await.unwrap();
        let mut queue = GitHubReadQueue::new(Arc::new(conn), SHORT_DURATION, b_ids(), "foo")
            .await
            .unwrap();

        const SHORT_DURATION: Duration = Duration::from_millis(100);

        let sent_msg = vec![1, 2, 3, 4, 5];
        for _ in 0..3 {
            b.write(bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap())
                .await
                .unwrap();
            b.write(sent_msg.clone()).await.unwrap();
        }

        for _ in 0..3 {
            let read_msg = queue.read_msg().await.unwrap().unwrap();
            assert_eq!(read_msg, sent_msg);
        }
    }

    #[tokio::test]
    async fn read_multiple_msgs_interleaved() {
        let conn = FakeConnection::default();
        let b = conn.create_blob("foo").await.unwrap();
        let mut queue = GitHubReadQueue::new(Arc::new(conn), SHORT_DURATION, b_ids(), "foo")
            .await
            .unwrap();

        let sent_msg = vec![1, 2, 3, 4, 5];
        for _ in 0..3 {
            b.write(bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap())
                .await
                .unwrap();
            b.write(sent_msg.clone()).await.unwrap();

            let read_msg = queue.read_msg().await.unwrap().unwrap();
            assert_eq!(read_msg, sent_msg);
        }
    }

    #[tokio::test]
    async fn read_ignores_keep_alive_msgs() {
        let conn = FakeConnection::default();
        let b = conn.create_blob("foo").await.unwrap();
        let mut queue = GitHubReadQueue::new(Arc::new(conn), SHORT_DURATION, b_ids(), "foo")
            .await
            .unwrap();

        let sent_msg = vec![1, 2, 3, 4, 5];
        for _ in 0..3 {
            b.write(bincode::serialize(&MessageHeader::KeepAlive).unwrap())
                .await
                .unwrap();
            b.write(bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap())
                .await
                .unwrap();
            b.write(sent_msg.clone()).await.unwrap();
        }

        for _ in 0..3 {
            let read_msg = queue.read_msg().await.unwrap().unwrap();
            assert_eq!(read_msg, sent_msg);
        }
    }

    #[tokio::test]
    async fn read_with_shutdown() {
        let conn = FakeConnection::default();
        let b = conn.create_blob("foo").await.unwrap();
        let mut queue = GitHubReadQueue::new(Arc::new(conn), SHORT_DURATION, b_ids(), "foo")
            .await
            .unwrap();

        let sent_msg = vec![1, 2, 3, 4, 5];
        b.write(bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap())
            .await
            .unwrap();
        b.write(sent_msg.clone()).await.unwrap();
        b.write(bincode::serialize(&MessageHeader::Shutdown).unwrap())
            .await
            .unwrap();

        let read_msg = queue.read_msg().await.unwrap().unwrap();
        assert_eq!(read_msg, sent_msg);
        assert_eq!(queue.read_msg().await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_timeout() {
        let conn = FakeConnection::default();
        let _ = conn.create_blob("foo").await.unwrap();
        let mut queue = GitHubReadQueue::new(Arc::new(conn), SHORT_DURATION, b_ids(), "foo")
            .await
            .unwrap();

        queue.read_msg().await.unwrap_err();
    }

    #[tokio::test]
    async fn write_msg() {
        let conn = FakeConnection::default();
        let mut queue = GitHubWriteQueue::new(&conn, FOREVER, "foo").await.unwrap();
        let sent = [1, 2, 3, 4, 5];
        queue.write_msg(&sent[..]).await.unwrap();

        let mut expected = bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap();
        expected.extend(sent);

        let b = conn.get_blob(b_ids(), "foo").await.unwrap();
        assert_eq!(b.data(), expected);
    }

    #[tokio::test]
    async fn write_many_msgs() {
        let conn = FakeConnection::default();
        let mut queue = GitHubWriteQueue::new(&conn, FOREVER, "foo").await.unwrap();
        let sent = vec![1, 2, 3, 4, 5];
        queue.write_many_msgs(&vec![sent.clone(); 3]).await.unwrap();

        let mut expected = vec![];
        for _ in 0..3 {
            expected.extend(bincode::serialize(&MessageHeader::Payload { size: 5 }).unwrap());
            expected.extend(sent.clone());
        }

        let b = conn.get_blob(b_ids(), "foo").await.unwrap();
        assert_eq!(b.data(), expected);
    }

    #[tokio::test]
    async fn write_shutdown() {
        let conn = FakeConnection::default();
        let mut queue = GitHubWriteQueue::new(&conn, FOREVER, "foo").await.unwrap();
        queue.shut_down().await.unwrap();

        let expected = bincode::serialize(&MessageHeader::Shutdown).unwrap();

        let b = conn.get_blob(b_ids(), "foo").await.unwrap();
        assert_eq!(b.data(), expected);
    }

    #[tokio::test]
    async fn keep_alive() {
        let conn = FakeConnection::default();
        let queue = GitHubWriteQueue::new(&conn, Duration::from_micros(1), "foo")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(queue);

        let b = conn.get_blob(b_ids(), "foo").await.unwrap();
        let data = b.data();
        let mut cursor = &data[..];

        let mut keep_alive_count = 0;
        while !cursor.is_empty() {
            let header: MessageHeader = bincode::deserialize_from(&mut cursor).unwrap();
            assert_eq!(header, MessageHeader::KeepAlive);
            keep_alive_count += 1;
        }

        assert!(keep_alive_count > 50, "{keep_alive_count}");
    }

    #[tokio::test]
    async fn accept_and_connect() {
        let conn = FakeConnection::default();

        let their_conn = conn.clone();
        tokio::task::spawn(async move {
            let mut acceptor = GitHubQueueAcceptor::new(their_conn, "foo").await.unwrap();
            let mut queue_b = acceptor.accept_one().await.unwrap();
            queue_b.write_msg(&b"hello"[..]).await.unwrap();
        });

        let mut queue_a = GitHubQueue::connect(conn, "foo").await.unwrap();
        let msg = queue_a.read_msg().await.unwrap().unwrap();
        assert_eq!(msg, b"hello");
    }

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
        let mut sock = GitHubQueue::connect(client, "foo").await.unwrap();
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
