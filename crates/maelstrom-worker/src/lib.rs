//! Code for the worker binary.

mod cache;
pub mod config;
mod dispatcher;
mod executor;
mod fetcher;
mod layer_fs;
mod reaper;

use anyhow::Result;
use cache::{Cache, StdCacheFs};
use config::{Config, InlineLimit};
use dispatcher::{Dispatcher, DispatcherDeps, Message};
use executor::Executor;
use maelstrom_base::{
    manifest::ManifestEntryData,
    proto::{Hello, WorkerToBroker},
    ArtifactType, JobError, JobId, JobResult, JobSpec, JobStatus, Sha256Digest,
};
use maelstrom_linux::{self as linux, Errno, Pid, Signal};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, CacheRoot},
    fs::Fs,
    manifest::AsyncManifestReader,
    net, sync,
};
use reaper::ReaperDeps;
use slog::{debug, error, info, o, warn, Logger};
use std::collections::HashSet;
use std::path::Path;
use std::{ops::ControlFlow, path::PathBuf, process, thread, time::Duration};
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

type DispatcherReceiver = UnboundedReceiver<Message>;
type DispatcherSender = UnboundedSender<Message>;
type BrokerSocketSender = UnboundedSender<WorkerToBroker>;

struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    broker_addr: BrokerAddr,
    inline_limit: InlineLimit,
    log: Logger,
    executor: Executor,
    cache_dir: PathBuf,
    tmpfs_dir: PathBuf,
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
        cache_dir: PathBuf,
    ) -> Result<Self> {
        let fs = Fs::new();
        fs.create_dir_all(&mount_dir)?;
        fs.create_dir_all(&tmpfs_dir)?;
        Ok(DispatcherAdapter {
            dispatcher_sender,
            broker_socket_sender,
            broker_addr,
            inline_limit,
            log,
            executor: Executor::new(mount_dir, tmpfs_dir.clone())?,
            cache_dir,
            tmpfs_dir,
        })
    }
}

struct DispatcherAdapterFuseHandle {
    inner: maelstrom_fuse::FuseHandle,
    mount_path: PathBuf,
}

impl DispatcherAdapterFuseHandle {
    async fn clean_up(self) -> Result<()> {
        self.inner.umount_and_join().await?;
        let fs = async_fs::Fs::new();
        fs.remove_dir(self.mount_path).await
    }
}

impl DispatcherDeps for DispatcherAdapter {
    type FuseHandle = DispatcherAdapterFuseHandle;

    fn start_job(
        &mut self,
        jid: JobId,
        spec: JobSpec,
        layer_fs_path: PathBuf,
    ) -> JobResult<(Pid, Self::FuseHandle), String> {
        let sender = self.dispatcher_sender.clone();
        let sender2 = sender.clone();
        let log = self
            .log
            .new(o!("jid" => format!("{jid:?}"), "spec" => format!("{spec:?}")));
        debug!(log, "job starting");
        let log2 = log.clone();

        let mount_path = self.tmpfs_dir.join(format!(
            "fuse_mount.{}.{}",
            jid.cid.as_u32(),
            jid.cjid.as_u32()
        ));
        let fs = Fs::new();
        fs.create_dir_all(&mount_path)
            .map_err(|e| JobError::System(e.to_string()))?;
        let spec = executor::JobSpec::from_spec_and_path(spec, mount_path.clone());

        let cache_dir = self.cache_dir.join("blob/sha256");
        let mount_path2 = mount_path.clone();
        let layer_fs = maelstrom_layer_fs::LayerFs::from_path(&layer_fs_path, &cache_dir)
            .map_err(|e| JobError::System(e.to_string()))?;
        let fuse_handle = layer_fs
            .mount(self.log.clone(), &mount_path2)
            .map_err(|e| JobError::System(e.to_string()))?;
        let fuse_handle = DispatcherAdapterFuseHandle {
            inner: fuse_handle,
            mount_path,
        };

        let pid = tokio::task::block_in_place(|| {
            self.executor.start(
                &spec,
                self.inline_limit,
                move |result| {
                    debug!(log, "job stdout"; "result" => ?result);
                    sender
                        .send(Message::JobStdout(jid, result.map_err(|e| e.to_string())))
                        .ok();
                },
                move |result| {
                    debug!(log2, "job stderr"; "result" => ?result);
                    sender2
                        .send(Message::JobStderr(jid, result.map_err(|e| e.to_string())))
                        .ok();
                },
            )
        })
        .map_err(|e| e.map(|inner| inner.to_string()))?;
        Ok((pid, fuse_handle))
    }

    fn kill_job(&mut self, pid: Pid) {
        linux::kill(pid, Signal::KILL).ok();
    }

    fn clean_up_fuse_handle_on_task(&mut self, handle: Self::FuseHandle) {
        let log = self.log.clone();
        task::spawn(async move {
            if let Err(e) = handle.clean_up().await {
                error!(log, "error unmounting fuse: {e}")
            }
        });
    }

    type TimerHandle = JoinHandle<()>;

    fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
        let sender = self.dispatcher_sender.clone();
        task::spawn(async move {
            time::sleep(duration).await;
            sender.send(Message::JobTimer(jid)).ok();
        })
    }

    fn cancel_timer(&mut self, handle: Self::TimerHandle) {
        handle.abort()
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
        let cache_path = self.cache_dir.join("blob/sha256");
        task::spawn(async move {
            debug!(log, "building bottom FS layer"; "layer_path" => ?layer_path);
            let result = layer_fs::build_bottom_layer(
                log.clone(),
                layer_path.clone(),
                cache_path,
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
        let cache_path = self.cache_dir.join("blob/sha256");
        task::spawn(async move {
            debug!(log, "building upper FS layer"; "layer_path" => ?layer_path);
            let result = layer_fs::build_upper_layer(
                log.clone(),
                layer_path.clone(),
                cache_path,
                lower_layer_path,
                upper_layer_path,
            )
            .await;
            debug!(log, "built upper FS layer"; "result" => ?result);
            sender.send(Message::BuiltUpperFsLayer(digest, result)).ok();
        });
    }

    fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
        let sender = self.dispatcher_sender.clone();
        let log = self.log.clone();
        task::spawn(async move {
            debug!(log, "reading digests from manifest"; "manifest_path" => ?path);
            let result = read_manifest(&path).await;
            debug!(log, "read digests from manifest"; "result" => ?result);
            sender
                .send(Message::ReadManifestDigests(digest, jid, result))
                .ok();
        });
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

    let cache = Cache::new(
        StdCacheFs,
        CacheRoot::from(cache_root.clone()),
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
        cache_root,
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

struct ReaperAdapter {
    log: Logger,
    sender: DispatcherSender,
}

impl ReaperDeps for ReaperAdapter {
    fn on_wait_error(&mut self, err: Errno) -> ControlFlow<()> {
        warn!(self.log, "waitid errored"; "err" => %err);
        ControlFlow::Continue(())
    }
    fn on_dummy_child_termination(&mut self) -> ControlFlow<()> {
        panic!("dummy child process terminated");
    }
    fn on_child_termination(&mut self, pid: Pid, status: JobStatus) -> ControlFlow<()> {
        debug!(self.log, "waitid returned"; "pid" => %pid, "status" => ?status);
        self.sender
            .send(Message::PidStatus(pid, status))
            .map_or(ControlFlow::Break(()), |_| ControlFlow::Continue(()))
    }
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

    let dummy_pid = reaper::clone_dummy_child()?;

    let reaper_adapter = ReaperAdapter {
        log: log.clone(),
        sender: dispatcher_sender.clone(),
    };
    thread::spawn(move || reaper::main(reaper_adapter, dummy_pid));

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
