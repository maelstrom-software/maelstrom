//! Code for the worker binary.

mod cache;
pub mod config;
mod dispatcher;
mod executor;
mod fetcher;
mod layer_fs;
pub mod local_worker;

use anyhow::{Context as _, Result};
use cache::{Cache, StdFs};
use config::Config;
use dispatcher::{Deps, Dispatcher, Message};
use executor::{Executor, MountDir, TmpfsDir};
use lru::LruCache;
use maelstrom_base::{
    manifest::ManifestEntryData,
    proto::{Hello, WorkerToBroker},
    ArtifactType, JobError, JobId, JobSpec, Sha256Digest,
};
use maelstrom_layer_fs::{BlobDir, LayerFs, ReaderCache};
use maelstrom_linux::{
    self as linux, CloneArgs, CloneFlags, PollEvents, PollFd, Signal, WaitStatus,
};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, InlineLimit},
    fs::Fs,
    manifest::AsyncManifestReader,
    net,
    root::RootBuf,
    sync::{self, EventReceiver, EventSender},
    time::SystemMonotonicClock,
};
use slog::{debug, error, info, o, Logger};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    path::Path,
    slice,
    sync::Arc,
    {path::PathBuf, process, thread, time::Duration},
};
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
pub type DispatcherSender = UnboundedSender<Message>;
type BrokerSocketSender = UnboundedSender<WorkerToBroker>;

pub struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    inline_limit: InlineLimit,
    log: Logger,
    executor: Arc<Executor<'static, SystemMonotonicClock>>,
    blob_dir: RootBuf<BlobDir>,
    layer_fs_cache: Arc<tokio::sync::Mutex<ReaderCache>>,
    manifest_digest_cache: ManifestDigestCache,
}

impl DispatcherAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dispatcher_sender: DispatcherSender,
        inline_limit: InlineLimit,
        log: Logger,
        mount_dir: RootBuf<MountDir>,
        tmpfs_dir: RootBuf<TmpfsDir>,
        blob_dir: RootBuf<BlobDir>,
    ) -> Result<Self> {
        let fs = Fs::new();
        fs.create_dir_all(&mount_dir)?;
        fs.create_dir_all(&tmpfs_dir)?;
        Ok(DispatcherAdapter {
            inline_limit,
            executor: Arc::new(Executor::new(mount_dir, tmpfs_dir, &SystemMonotonicClock)?),
            blob_dir,
            layer_fs_cache: Arc::new(tokio::sync::Mutex::new(ReaderCache::new())),
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

        let layer_fs = LayerFs::from_path(&layer_fs_path, self.blob_dir.as_root())?;
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

pub struct TimerHandle(JoinHandle<()>);

impl Drop for TimerHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Deps for DispatcherAdapter {
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

    fn build_bottom_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: PathBuf,
        artifact_type: ArtifactType,
        artifact_path: PathBuf,
    ) {
        let sender = self.dispatcher_sender.clone();
        let log = self.log.clone();
        let blob_dir = self.blob_dir.clone();
        task::spawn(async move {
            debug!(log, "building bottom FS layer"; "layer_path" => ?layer_path);
            let result = layer_fs::build_bottom_layer(
                log.clone(),
                layer_path.clone(),
                blob_dir.as_root(),
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
        let blob_dir = self.blob_dir.clone();
        task::spawn(async move {
            debug!(log, "building upper FS layer"; "layer_path" => ?layer_path);
            let result = layer_fs::build_upper_layer(
                log.clone(),
                layer_path.clone(),
                blob_dir.as_root(),
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
}

struct ArtifactFetcher {
    dispatcher_sender: DispatcherSender,
    broker_addr: BrokerAddr,
    log: Logger,
}

impl ArtifactFetcher {
    fn new(dispatcher_sender: DispatcherSender, broker_addr: BrokerAddr, log: Logger) -> Self {
        ArtifactFetcher {
            broker_addr,
            dispatcher_sender,
            log,
        }
    }
}

impl dispatcher::ArtifactFetcher for ArtifactFetcher {
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
            sender
                .send(Message::ArtifactFetchCompleted(digest, result))
                .ok();
        });
    }
}

struct BrokerSender {
    broker_socket_sender: BrokerSocketSender,
}

impl BrokerSender {
    fn new(broker_socket_sender: BrokerSocketSender) -> Self {
        Self {
            broker_socket_sender,
        }
    }
}

impl dispatcher::BrokerSender for BrokerSender {
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
    let mount_dir = config.cache_root.join("mount").transmute::<MountDir>();
    let tmpfs_dir = config.cache_root.join("upper").transmute::<TmpfsDir>();
    let cache_root = config.cache_root.join("artifacts");
    let blob_dir = cache_root.join("blob/sha256").transmute::<BlobDir>();

    let broker_sender = BrokerSender::new(broker_socket_sender);
    let cache = Cache::new(StdFs, cache_root, config.cache_size, log.clone());
    let artifact_fetcher =
        ArtifactFetcher::new(dispatcher_sender.clone(), config.broker, log.clone());
    match DispatcherAdapter::new(
        dispatcher_sender,
        config.inline_limit,
        log.clone(),
        mount_dir,
        tmpfs_dir,
        blob_dir,
    ) {
        Err(err) => {
            error!(log, "could not start executor"; "err" => ?err);
        }
        Ok(adapter) => {
            let mut dispatcher = Dispatcher::new(
                adapter,
                artifact_fetcher,
                broker_sender,
                cache,
                config.slots,
            );
            let _ =
                sync::channel_reader(dispatcher_receiver, |msg| dispatcher.receive_message(msg))
                    .await;
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
#[tokio::main]
pub async fn main_inner(config: Config, log: Logger) -> Result<()> {
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
    join_set.spawn(async move {
        let _ = net::async_socket_reader(read_stream, dispatcher_sender_clone, move |msg| {
            debug!(log_clone, "received broker message"; "msg" => ?msg);
            Message::Broker(msg)
        })
        .await;
    });
    let log_clone = log.clone();
    join_set.spawn(async move {
        let _ = net::async_socket_writer(broker_socket_receiver, write_stream, move |msg| {
            debug!(log_clone, "sending broker message"; "msg" => ?msg);
        })
        .await;
    });
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

pub fn main(config: Config, log: Logger) -> Result<()> {
    main_inner(config, log)
}

fn mimic_child_death(status: WaitStatus) -> ! {
    match status {
        WaitStatus::Exited(code) => {
            process::exit(code.as_u8().into());
        }
        WaitStatus::Signaled(signal) => {
            linux::raise(signal).unwrap_or_else(|e| panic!("error raising signal {signal}: {e}"));
            // Maybe, for some reason, we didn't actually terminate on the signal from above. In
            // that case, just abort.
            process::abort();
        }
    }
}

/// Create a grandchild process in its own pid and user namespaces.
///
/// We want to run the worker in its own pid namespace so that when it terminates, all descendant
/// processes also terminate. We don't want the worker to ever leak jobs, no matter how it
/// terminates.
///
/// We have to create a user namespace so that we can create the pid namespace.
///
/// We do two levels of cloning so that the returned process isn't pid 1 in its own pid namespace.
/// This is important because we don't want that process to inherit orphaned descendants. We want
/// to worker to be able to effectively use waitpid (or equivalently, wait on pidfds). If the
/// worker had to worry about reaping zombie descendants, then it would need to call the generic
/// wait functions, which could return a pid for one of the legitimate children that the process
/// was trying to waidpid on. This makes calling waitpid a no-go, and complicates the design of
/// the worker.
///
/// It's much easier to just have the worker not be pid 1 in its own namespace.
///
/// We take special care to ensure that all three processes will terminate if any single process
/// terminates. This is accomplished in the following ways:
///   - If the gen 0 process (the calling process) terminates, the gen 1 process will receive a
///     parent-death signal, which will immediately terminate it.
///   - If the gen 1 process terminates, all processes in the new pid namespace -- including the
///     gen 2 process -- will immediately
///     terminate, since the gen 1 process had pid 1 in their pid namespace
///   - After creating the gen 2 process, the gen 1 process loops calling wait(2) forever. It does
///     this to reap zombies, but it also allows it to detect when the gen 2 process terminates.
///     When this happens, the gen 1 process will terminate itself, trying to mimic the termination
///     mode of the gen 2 process.
///   - After cloning the gen 1 process, the gen 0 process just calls waitpid on the gen 1 process.
///     When this returns, it knows the gen 1 process has terminated. It then terminates itself,
///     trying to mimic the termination mode of the gen 1 process.
///
/// This function will return exactly once. It may be the gen 0, gen 1, or gen 2 process. It'll be
/// in the gen 2 process if and only if it returns `Ok(())`. It'll be in the gen 0 or gen 1 process
/// if and only if it return `Err(_)`.
///
/// WARNING: This function must only be called while the program is single-threaded. We check this
/// and will panic if called when there is more than one thread.
pub fn clone_into_pid_and_user_namespace() -> Result<()> {
    maelstrom_util::thread::assert_single_threaded()?;

    let gen_0_uid = linux::getuid();
    let gen_0_gid = linux::getgid();

    // Create a pidfd for the gen 0 process. We'll use this in the gen 1 process to see if the gen
    // 0 process has terminated early.
    let gen_0_pidfd = linux::pidfd_open(linux::getpid())?;

    // Clone a new process into new user, pid, and mount namespaces.
    let mut clone_args = CloneArgs::default()
        .flags(CloneFlags::NEWUSER | CloneFlags::NEWPID)
        .exit_signal(Signal::CHLD);
    match linux::clone3(&mut clone_args).context("cloning the second-generation process")? {
        Some(gen_1_pid) => {
            // Gen 0 process.

            // The gen_0_pidfd is only used in the gen 1 process.
            drop(gen_0_pidfd);

            // Wait for the gen 1 process's termination and mimic how it terminated.
            mimic_child_death(linux::waitpid(gen_1_pid).unwrap_or_else(|e| {
                panic!("error waiting on second-generation process {gen_1_pid}: {e}")
            }));
        }
        None => {
            // Gen 1 process.

            // Set parent death signal.
            linux::prctl_set_pdeathsig(Signal::KILL)?;

            // Check if the gen 0 process has already terminated. We do this to deal with a race
            // condition. It's possible for the gen 0 process to terminate before we call prctl
            // above. If that happens, we won't receive a death signal until our new parent
            // terminates, which is not what we want, as that new parent is probably the system
            // init daemon.
            //
            // Unfortunately, we can't attempt to see if the gen 0 process is still alive using its
            // pid, as it's not in our pid namespace. We get around this by inheriting a pidfd from
            // the gen 0 process. The pidfd will become readable once the process has terminated.
            // So, we can just do a non-blocking poll on the fd to see fi the gen 0 process has
            // already terminated. If it hasn't, we can rely on the parent death signal mechanism.
            let mut pollfd = PollFd::new(gen_0_pidfd.as_fd(), PollEvents::IN);
            if linux::poll(slice::from_mut(&mut pollfd), Duration::ZERO)? == 1 {
                process::abort();
            }

            // We are done with the parent_pidfd now.
            drop(gen_0_pidfd);

            // Map uid and guid. If we don't do this here, then children processes will not be able
            // to map their own uids and gids.
            let fs = Fs::new();
            fs.write("/proc/self/setgroups", "deny\n")?;
            fs.write("/proc/self/uid_map", format!("0 {gen_0_uid} 1\n"))?;
            fs.write("/proc/self/gid_map", format!("0 {gen_0_gid} 1\n"))?;

            // Fork the gen 2 process.
            match linux::fork()? {
                Some(gen_2_pid) => {
                    loop {
                        // Gen 1 process.

                        // Now that we've created the gen 2 process, we need to start reaping
                        // zombies. If we ever notice that the gen 2 process terminated, then it's
                        // time to terminate ourselves.
                        match linux::wait() {
                            Err(err) => panic!("error waiting: {err}"),
                            Ok(result) if result.pid == gen_2_pid => {
                                mimic_child_death(result.status)
                            }
                            Ok(_) => {}
                        }
                    }
                }
                None => {
                    // Gen 2 process.
                    Ok(())
                }
            }
        }
    }
}
