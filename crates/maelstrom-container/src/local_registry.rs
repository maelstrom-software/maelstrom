use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use http_body::Frame;
use http_body_util::{combinators::BoxBody, BodyExt as _, Empty, Full, StreamBody};
use hyper::{body::Incoming, server::conn::http1, service::Service, Request, Response};
use hyper_util::rt::tokio::TokioIo;
use maelstrom_util::async_fs::Fs;
use std::{
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::{pin, Pin},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncSeekExt as _, SeekFrom},
    net::TcpListener,
};

fn canned_cert() -> &'static [u8] {
    include_bytes!("local_registry_cert.pem")
}

fn canned_key() -> &'static [u8] {
    include_bytes!("local_registry_key.pem")
}

fn empty() -> BoxBody<Bytes, Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

async fn get_from_tar_stream(
    path: impl AsRef<Path>,
    tar_path: impl AsRef<Path>,
) -> Result<impl Stream<Item = Result<Vec<u8>>> + 'static> {
    let fs = Fs::new();
    let mut archive = tokio_tar::Archive::new(fs.open_file(path).await?);
    let mut entries = archive.entries()?;
    let mut found = None;
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        if entry.path()? == tar_path.as_ref() {
            let size = entry.header().entry_size()?;
            let file_pos = entry.raw_file_position();
            found = Some(file_pos..(file_pos + size));
        }
    }
    let Some(range) = found else {
        bail!("{} not found", tar_path.as_ref().display());
    };
    drop(entries);

    let mut file = archive.into_inner().map_err(drop).unwrap();
    file.seek(SeekFrom::Start(range.start)).await?;

    let remaining = range.end - range.start;
    Ok(futures::stream::try_unfold(
        (file, remaining),
        |(mut file, mut remaining)| async move {
            if remaining == 0 {
                Ok(None)
            } else {
                let to_read = std::cmp::min(1024, remaining);
                let mut buffer = vec![0; to_read as usize];
                file.read_exact(&mut buffer).await?;
                remaining -= to_read;
                Ok(Some((buffer, (file, remaining))))
            }
        },
    ))
}

async fn get_from_tar(path: impl AsRef<Path>, tar_path: impl AsRef<Path>) -> Result<String> {
    let stream = get_from_tar_stream(path, tar_path).await?;
    let mut pinned_stream = pin!(stream);

    let mut bytes = vec![];
    while let Some(chunk) = pinned_stream.next().await {
        bytes.extend(chunk?);
    }
    Ok(String::from_utf8(bytes)?)
}

pub struct LocalRegistry {
    source_dir: PathBuf,
    listener: TcpListener,
    log: slog::Logger,
}

impl LocalRegistry {
    pub async fn new(source_dir: impl Into<PathBuf>, log: slog::Logger) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Ok(Self {
            source_dir: source_dir.into(),
            listener,
            log,
        })
    }

    pub async fn run(source_dir: impl Into<PathBuf>, log: slog::Logger) -> Result<SocketAddr> {
        Self::run_inner(source_dir, log).await.map(|(addr, _)| addr)
    }

    async fn run_inner(
        source_dir: impl Into<PathBuf>,
        log: slog::Logger,
    ) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let self_ = Self::new(source_dir, log).await?;
        let address = self_.address()?;
        let handle = tokio::task::spawn(async move { self_.run_until_error().await.unwrap() });
        Ok((address, handle))
    }

    #[tokio::main]
    async fn run_with_runtime(
        source_dir: impl Into<PathBuf>,
        log: slog::Logger,
        send: tokio::sync::oneshot::Sender<Result<SocketAddr>>,
    ) {
        match Self::run_inner(source_dir, log).await {
            Ok((address, handle)) => {
                send.send(Ok(address)).ok();
                handle.await.unwrap();
            }
            Err(error) => {
                send.send(Err(error)).ok();
            }
        }
    }

    pub fn run_sync(source_dir: impl Into<PathBuf>, log: slog::Logger) -> Result<SocketAddr> {
        let (send, recv) = tokio::sync::oneshot::channel();
        let source_dir = source_dir.into();
        std::thread::spawn(move || Self::run_with_runtime(source_dir, log, send));
        recv.blocking_recv().unwrap()
    }

    pub fn address(&self) -> Result<SocketAddr> {
        let addr = self.listener.local_addr()?;
        Ok(addr)
    }

    pub async fn run_until_error(self) -> Result<()> {
        let self_ = Arc::new(self);

        let identity = native_tls::Identity::from_pkcs8(canned_cert(), canned_key())?;
        let tls_acceptor =
            tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::new(identity)?);

        loop {
            let (stream, _) = self_.listener.accept().await?;
            let self_clone = self_.clone();
            let tls_acceptor = tls_acceptor.clone();
            tokio::task::spawn(async move {
                let stream = match tls_acceptor.accept(stream).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        slog::error!(self_clone.log, "failure serving TLS"; "error" => %err);
                        return;
                    }
                };
                let handle = LocalRegistryHandle {
                    handle: self_clone.clone(),
                };
                if let Err(err) = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), handle)
                    .await
                {
                    slog::error!(self_clone.log, "failure serving HTTP"; "error" => %err);
                }
            });
        }
    }

    async fn get_manifest(
        &self,
        image_name: &str,
        accept: Vec<oci_spec::image::MediaType>,
        manfiest_name: &str,
    ) -> Result<Response<BoxBody<Bytes, Error>>> {
        let fs = Fs::new();
        let tar_path = self.source_dir.join(format!("{image_name}.tar"));
        if !fs.exists(&tar_path).await {
            slog::error!(self.log, "not found"; "path" => ?tar_path);
            return Ok(Response::builder().status(404).body(empty()).unwrap());
        }

        if accept
            .iter()
            .any(|h| h == &oci_spec::image::MediaType::ImageIndex)
        {
            let res = get_from_tar(&tar_path, "index.json").await?;
            let mut index: oci_spec::image::ImageIndex = serde_json::from_str(&res)?;
            index.set_media_type(Some(oci_spec::image::MediaType::ImageIndex));

            let content_type = index
                .media_type()
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or("application/json".into());
            Ok(Response::builder()
                .status(200)
                .header("Content-Type", content_type)
                .body(full(res))
                .unwrap())
        } else if accept
            .iter()
            .any(|h| h == &oci_spec::image::MediaType::ImageManifest)
        {
            if let Some((algo, manifest_digest)) = manfiest_name.split_once(':') {
                let res =
                    get_from_tar(&tar_path, format!("blobs/{algo}/{manifest_digest}")).await?;
                let manifest: oci_spec::image::ImageManifest = serde_json::from_str(&res)?;
                let content_type = manifest
                    .media_type()
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or("application/json".into());
                Ok(Response::builder()
                    .status(200)
                    .header("Content-Type", content_type)
                    .body(full(res))
                    .unwrap())
            } else {
                Ok(Response::builder().status(404).body(empty()).unwrap())
            }
        } else {
            Ok(Response::builder().status(404).body(empty()).unwrap())
        }
    }

    async fn get_blob(
        &self,
        image_name: &str,
        digest: &str,
    ) -> Result<Response<BoxBody<Bytes, Error>>> {
        let fs = Fs::new();
        let tar_path = self.source_dir.join(format!("{image_name}.tar"));
        if !fs.exists(&tar_path).await {
            slog::error!(self.log, "not found"; "path" => ?tar_path);
            return Ok(Response::builder().status(404).body(empty()).unwrap());
        }
        let (algo, digest) = digest
            .split_once(':')
            .ok_or_else(|| anyhow!("bad digest"))?;

        let stream = get_from_tar_stream(&tar_path, format!("blobs/{algo}/{digest}"))
            .await?
            .map(|res| res.map(Bytes::from).map(Frame::data));
        Ok(Response::builder()
            .status(200)
            .body(BoxBody::new(StreamBody::new(stream)))
            .unwrap())
    }
}

struct LocalRegistryHandle {
    handle: Arc<LocalRegistry>,
}

impl Service<Request<Incoming>> for LocalRegistryHandle {
    type Response = Response<BoxBody<Bytes, Error>>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let self_clone = self.handle.clone();
        Box::pin(async move {
            let path = req.uri().path();
            slog::info!(self_clone.log, "request"; "path" => path);
            let parts: Vec<_> = path.split('/').filter(|s| !s.is_empty()).collect();
            if parts[0] != "v2" {
                bail!("bad version {}", parts[0]);
            }
            let accept = req
                .headers()
                .get_all("Accept")
                .iter()
                .filter_map(|h| Some(oci_spec::image::MediaType::from(h.to_str().ok()?)))
                .collect();
            Ok(match &parts[1..] {
                &[image_name, "manifests", tag] => {
                    self_clone.get_manifest(image_name, accept, tag).await?
                }
                &[image_name, "blobs", hash] => self_clone.get_blob(image_name, hash).await?,
                &[] => Response::builder().status(200).body(empty()).unwrap(),
                a => {
                    bail!("unexpected request {a:?}")
                }
            })
        })
    }
}
