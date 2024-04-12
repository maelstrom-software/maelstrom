//! Code for the worker binary.

mod cache;
pub mod config;
mod dispatcher;
mod executor;
mod fetcher;
mod layer_fs;

use anyhow::Result;
use cache::{Cache, StdCacheFs};
use config::{Config, InlineLimit};
use dispatcher::{Dispatcher, DispatcherDeps, Message};
use executor::Executor;
use lru::LruCache;
use maelstrom_base::{
    manifest::ManifestEntryData,
    proto::{Hello, WorkerToBroker},
    ArtifactType, JobError, JobId, JobSpec, Sha256Digest,
};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, CacheRoot},
    fs::Fs,
    manifest::AsyncManifestReader,
    net,
    sync::{self, EventReceiver, EventSender},
};
use slog::{debug, error, info, o, Logger};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::{path::PathBuf, process, thread, time::Duration};
use tokio::{
    io::BufReader,
    net::TcpStream,
    signal::unix::{self, SignalKind},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::{self, JoinHandle, JoinSet},
    time,
};

async fn read_manifest(path: &Path) -> Result<HashSet<Sha256Digest>> {
    let fs = async_fs::Fs::new();
    let mut reader = AsyncManifestReader::new(fs.open_file(path).await?).await?;
    let mut digests = HashSet::new();
    while let Some(entry) = reader.next().await? {
        if let ManifestEntryData::File(Some(digest)) = entry.data {
            digests.insert(digest);
        }
    }
    Ok(digests)
}

struct ManifestDigestCacheInner {
    pending: HashMap<PathBuf, Vec<(Sha256Digest, JobId)>>,
    cached: LruCache<PathBuf, HashSet<Sha256Digest>>,
}

impl ManifestDigestCacheInner {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            pending: HashMap::new(),
            cached: LruCache::new(capacity),
        }
    }
}

#[derive(Clone)]
struct ManifestDigestCache {
    sender: DispatcherSender,
    log: slog::Logger,
    cache: Arc<std::sync::Mutex<ManifestDigestCacheInner>>,
}

impl ManifestDigestCache {
    fn new(sender: DispatcherSender, log: slog::Logger, capacity: NonZeroUsize) -> Self {
        Self {
            sender,
            log,
            cache: Arc::new(std::sync::Mutex::new(ManifestDigestCacheInner::new(
                capacity,
            ))),
        }
    }

    fn get(&self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
        let mut locked_cache = self.cache.lock().unwrap();
        if let Some(waiting) = locked_cache.pending.get_mut(&path) {
            waiting.push((digest, jid));
        } else if let Some(cached) = locked_cache.cached.get(&path) {
            self.sender
                .send(Message::ReadManifestDigests(
                    digest,
                    jid,
                    Ok(cached.clone()),
                ))
                .ok();
        } else {
            locked_cache
                .pending
                .insert(path.clone(), vec![(digest, jid)]);

            let self_clone = self.clone();
            task::spawn(async move {
                self_clone.fill_cache(path).await;
            });
        }
    }

    async fn fill_cache(&self, path: PathBuf) {
        debug!(self.log, "reading digests from manifest"; "manifest_path" => ?path);
        let result = read_manifest(&path).await;
        debug!(self.log, "read digests from manifest"; "result" => ?result);

        let mut locked_cache = self.cache.lock().unwrap();
        let waiting = locked_cache.pending.remove(&path).unwrap();
        for (digest, jid) in waiting {
            // This is a clippy bug <https://github.com/rust-lang/rust-clippy/issues/12357>
            #[allow(clippy::useless_asref)]
            let res = result
                .as_ref()
                .map(|v| v.clone())
                .map_err(|e| anyhow::Error::msg(e.to_string()));
            self.sender
                .send(Message::ReadManifestDigests(digest, jid, res))
                .ok();
        }
        if let Ok(digests) = result {
            locked_cache.cached.push(path, digests);
        }
    }
}

const MANIFEST_DIGEST_CACHE_SIZE: usize = 10_000;

type DispatcherReceiver = UnboundedReceiver<Message>;
type DispatcherSender = UnboundedSender<Message>;
type BrokerSocketSender = UnboundedSender<WorkerToBroker>;

struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    broker_addr: BrokerAddr,
    inline_limit: InlineLimit,
    log: Logger,
    executor: Arc<Executor>,
    blob_cache_dir: PathBuf,
    layer_fs_cache: Arc<tokio::sync::Mutex<maelstrom_layer_fs::ReaderCache>>,
    manifest_digest_cache: ManifestDigestCache,
}

impl DispatcherAdapter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        dispatcher_sender: DispatcherSender,
        broker_socket_sender: BrokerSocketSender,
        broker_addr: BrokerAddr,
        inline_limit: InlineLimit,
        log: Logger,
        mount_dir: PathBuf,
        tmpfs_dir: PathBuf,
        blob_cache_dir: PathBuf,
    ) -> Result<Self> {
        let fs = Fs::new();
        fs.create_dir_all(&mount_dir)?;
        fs.create_dir_all(&tmpfs_dir)?;
        Ok(DispatcherAdapter {
            broker_socket_sender,
            broker_addr,
            inline_limit,
            executor: Arc::new(Executor::new(mount_dir, tmpfs_dir.clone())?),
            blob_cache_dir,
            layer_fs_cache: Arc::new(tokio::sync::Mutex::new(
                maelstrom_layer_fs::ReaderCache::new(),
            )),
            manifest_digest_cache: ManifestDigestCache::new(
                dispatcher_sender.clone(),
                log.clone(),
                MANIFEST_DIGEST_CACHE_SIZE.try_into().unwrap(),
            ),
            dispatcher_sender,
            log,
        })
    }

    fn start_job_inner(
        &mut self,
        jid: JobId,
        spec: JobSpec,
        layer_fs_path: PathBuf,
        kill_event_receiver: EventReceiver,
    ) -> Result<()> {
        let log = self
            .log
            .new(o!("jid" => format!("{jid:?}"), "spec" => format!("{spec:?}")));
        debug!(log, "job starting");

        let layer_fs =
            maelstrom_layer_fs::LayerFs::from_path(&layer_fs_path, &self.blob_cache_dir)?;
        let layer_fs_cache = self.layer_fs_cache.clone();
        let fuse_spawn = move |fd| {
            tokio::spawn(async move {
                if let Err(e) = layer_fs.run_fuse(log.clone(), layer_fs_cache, fd).await {
                    slog::error!(log, "FUSE handling got error {e:?}");
                }
            });
        };

        let executor = self.executor.clone();
        let spec = executor::JobSpec::from_spec(spec);
        let inline_limit = self.inline_limit;
        let dispatcher_sender = self.dispatcher_sender.clone();
        let runtime = tokio::runtime::Handle::current();
        task::spawn_blocking(move || {
            dispatcher_sender
                .send(Message::JobCompleted(
                    jid,
                    executor
                        .run_job(
                            &spec,
                            inline_limit,
                            kill_event_receiver,
                            fuse_spawn,
                            runtime,
                        )
                        .map_err(|e| e.map(|inner| inner.to_string())),
                ))
                .ok()
        });
        Ok(())
    }
}

struct TimerHandle(JoinHandle<()>);

impl Drop for TimerHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl DispatcherDeps for DispatcherAdapter {
    type JobHandle = EventSender;

    fn start_job(&mut self, jid: JobId, spec: JobSpec, layer_fs_path: PathBuf) -> Self::JobHandle {
        let (kill_event_sender, kill_event_receiver) = sync::event();
        if let Err(e) = self.start_job_inner(jid, spec, layer_fs_path, kill_event_receiver) {
            let _ = self.dispatcher_sender.send(Message::JobCompleted(
                jid,
                Err(JobError::System(e.to_string())),
            ));
        }
        kill_event_sender
    }

    type TimerHandle = TimerHandle;

    fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
        let sender = self.dispatcher_sender.clone();
        TimerHandle(task::spawn(async move {
            time::sleep(duration).await;
            sender.send(Message::JobTimer(jid)).ok();
        }))
    }

    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
        let sender = self.dispatcher_sender.clone();
        let broker_addr = self.broker_addr;
        let mut log = self.log.new(o!(
            "digest" => digest.to_string(),
            "broker_addr" => broker_addr.inner().to_string()
        ));
        debug!(log, "artifact fetcher starting");
        thread::spawn(move || {
            let result = fetcher::main(&digest, path, broker_addr, &mut log);
            debug!(log, "artifact fetcher completed"; "result" => ?result);
            sender.send(Message::ArtifactFetcher(digest, result)).ok();
        });
    }

    fn build_bottom_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: PathBuf,
        artifact_type: ArtifactType,
        artifact_path: PathBuf,
    ) {
        let sender = self.dispatcher_sender.clone();
        let log = self.log.clone();
        let blob_cache_dir = self.blob_cache_dir.clone();
        task::spawn(async move {
            debug!(log, "building bottom FS layer"; "layer_path" => ?layer_path);
            let result = layer_fs::build_bottom_layer(
                log.clone(),
                layer_path.clone(),
                &blob_cache_dir,
                digest.clone(),
                artifact_type,
                artifact_path,
            )
            .await;
            debug!(log, "built bottom FS layer"; "result" => ?result);
            sender
                .send(Message::BuiltBottomFsLayer(digest, result))
                .ok();
        });
    }

    fn build_upper_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: PathBuf,
        lower_layer_path: PathBuf,
        upper_layer_path: PathBuf,
    ) {
        let sender = self.dispatcher_sender.clone();
        let log = self.log.clone();
        let blob_cache_dir = self.blob_cache_dir.clone();
        task::spawn(async move {
            debug!(log, "building upper FS layer"; "layer_path" => ?layer_path);
            let result = layer_fs::build_upper_layer(
                log.clone(),
                layer_path.clone(),
                &blob_cache_dir,
                lower_layer_path,
                upper_layer_path,
            )
            .await;
            debug!(log, "built upper FS layer"; "result" => ?result);
            sender.send(Message::BuiltUpperFsLayer(digest, result)).ok();
        });
    }

    fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
        self.manifest_digest_cache.get(digest, path, jid);
    }

    fn send_message_to_broker(&mut self, message: WorkerToBroker) {
        self.broker_socket_sender.send(message).ok();
    }
}

async fn dispatcher_main(
    config: Config,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    log: Logger,
) {
    let mount_dir = config.cache_root.inner().join("mount");
    let tmpfs_dir = config.cache_root.inner().join("upper");
    let cache_root = config.cache_root.inner().join("artifacts");
    let blob_cache_dir = cache_root.join("blob/sha256");

    let cache = Cache::new(
        StdCacheFs,
        CacheRoot::from(cache_root),
        config.cache_size,
        log.clone(),
    );
    match DispatcherAdapter::new(
        dispatcher_sender,
        broker_socket_sender,
        config.broker,
        config.inline_limit,
        log.clone(),
        mount_dir,
        tmpfs_dir,
        blob_cache_dir,
    ) {
        Err(err) => {
            error!(log, "could not start executor"; "err" => ?err);
        }
        Ok(adapter) => {
            let mut dispatcher = Dispatcher::new(adapter, cache, config.slots);
            sync::channel_reader(dispatcher_receiver, |msg| dispatcher.receive_message(msg)).await
        }
    }
}

async fn signal_handler(kind: SignalKind, log: Logger, signame: &'static str) {
    unix::signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
    error!(log, "received {signame}")
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
pub async fn main(config: Config, log: Logger) -> Result<()> {
    info!(log, "started"; "config" => ?config, "pid" => process::id());

    let (read_stream, mut write_stream) = TcpStream::connect(config.broker.inner())
        .await
        .map_err(|err| {
            error!(log, "error connecting to broker"; "err" => %err);
            err
        })?
        .into_split();
    let read_stream = BufReader::new(read_stream);

    net::write_message_to_async_socket(
        &mut write_stream,
        Hello::Worker {
            slots: (*config.slots.inner()).into(),
        },
    )
    .await
    .map_err(|err| {
        error!(log, "error writing hello message"; "err" => %err);
        err
    })?;

    let (dispatcher_sender, dispatcher_receiver) = mpsc::unbounded_channel();
    let (broker_socket_sender, broker_socket_receiver) = mpsc::unbounded_channel();

    let mut join_set = JoinSet::new();

    let log_clone = log.clone();
    let dispatcher_sender_clone = dispatcher_sender.clone();
    join_set.spawn(net::async_socket_reader(
        read_stream,
        dispatcher_sender_clone,
        move |msg| {
            debug!(log_clone, "received broker message"; "msg" => ?msg);
            Message::Broker(msg)
        },
    ));
    let log_clone = log.clone();
    join_set.spawn(net::async_socket_writer(
        broker_socket_receiver,
        write_stream,
        move |msg| {
            debug!(log_clone, "sending broker message"; "msg" => ?msg);
        },
    ));
    join_set.spawn(dispatcher_main(
        config,
        dispatcher_receiver,
        dispatcher_sender,
        broker_socket_sender,
        log.clone(),
    ));
    join_set.spawn(signal_handler(
        SignalKind::interrupt(),
        log.clone(),
        "SIGINT",
    ));
    join_set.spawn(signal_handler(
        SignalKind::terminate(),
        log.clone(),
        "SIGTERM",
    ));

    join_set.join_next().await;
    info!(log, "exiting");
    Ok(())
}
