//! Central processing module for the worker. Receive messages from the broker, executors, and
//! artifact fetchers. Start or cancel jobs as appropriate via executors.

mod tracker;

use crate::cache::{self, GetArtifact, GotArtifact};
use anyhow::{Error, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    ArtifactType, JobCompleted, JobError, JobId, JobOutcome, JobResult, JobSpec, JobWorkerStatus,
    Sha256Digest,
};
use maelstrom_util::{config::common::Slots, duration, ext::OptionExt as _};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};
use tracker::{FetcherResult, LayerTracker};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// The external dependencies for [`Dispatcher`]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait Deps {
    /// The job handle should kill an outstanding job when it is dropped. Even if the job is
    /// killed, the `Dispatcher` should always be called with [`Message::JobCompleted`]. It must be
    /// safe to drop this handle after the job has completed.
    type JobHandle;

    type Fs: cache::Fs;

    /// Start a new job. The dispatcher expects a [`Message::JobCompleted`] message when the job
    /// completes.
    fn start_job(&mut self, jid: JobId, spec: JobSpec, path: PathBuf) -> Self::JobHandle;

    /// The timer handle should cancel an outstanding timer when it is dropped. It must be safe to
    /// drop this handle after the timer has completed. Dropping this handle may or may not result
    /// in no [`Message::JobTimer`] message. The dispatcher must be prepared to handle the case
    /// where the message arrives even after this handle has been dropped.
    type TimerHandle;

    /// Start a timer that will send a [`Message::JobTimer`] message when it's done, unless it is
    /// canceled beforehand.
    fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle;

    /// Start a task that will build a layer-fs bottom layer out of an artifact
    fn build_bottom_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: <Self::Fs as cache::Fs>::TempDir,
        artifact_type: ArtifactType,
        artifact_path: PathBuf,
    );

    /// Start a task that will build a layer-fs upper layer by stacking a bottom layer
    fn build_upper_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: <Self::Fs as cache::Fs>::TempDir,
        lower_layer_path: PathBuf,
        upper_layer_path: PathBuf,
    );

    /// Start a task to read the digests out of the given path to a manfiest.
    fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId);
}

/// The artifact fetcher is split out of [`Deps`] for convenience. The rest of [`Deps`] can stay
/// the same for "real" and local workers, but the artifact fetching is different
pub trait ArtifactFetcher<FsT: cache::Fs> {
    /// Start a thread that will download an artifact from the broker.
    fn start_artifact_fetch(&mut self, cache: &impl Cache<FsT>, digest: Sha256Digest);
}

/// The broker sender is split out of [`Deps`] for convenience. The rest of [`Deps`] can stay
/// the same for "real" and local workers, but sending messages to the broker is different. For the
/// "real" worker, we just enqueue the message as it is, but for the local worker, we need to wrap
/// it in a local-broker-specific message.
pub trait BrokerSender {
    /// Send a message to the broker.
    fn send_message_to_broker(&mut self, message: WorkerToBroker);

    /// Close the connection. All further messages will be black-holed.
    fn close(&mut self);
}

/// The [`Cache`] dependency for [`Dispatcher`]. This should be exactly the same as [`Cache`]'s
/// public interface. We have this so we can isolate [`Dispatcher`] when testing.
pub trait Cache<FsT: cache::Fs> {
    fn get_artifact(
        &mut self,
        kind: cache::EntryKind,
        artifact: Sha256Digest,
        jid: JobId,
    ) -> GetArtifact;
    fn got_artifact_failure(&mut self, kind: cache::EntryKind, digest: &Sha256Digest)
        -> Vec<JobId>;
    fn got_artifact_success(
        &mut self,
        kind: cache::EntryKind,
        digest: &Sha256Digest,
        artifact: GotArtifact<FsT>,
    ) -> Vec<JobId>;
    fn decrement_ref_count(&mut self, kind: cache::EntryKind, digest: &Sha256Digest);
    fn cache_path(&self, kind: cache::EntryKind, digest: &Sha256Digest) -> PathBuf;
    fn temp_file(&self) -> FsT::TempFile;
    fn temp_dir(&self) -> FsT::TempDir;
}

/// The standard implementation of [`Cache`] that just calls into [`cache::Cache`].
impl Cache<cache::StdFs> for cache::Cache<cache::StdFs> {
    fn get_artifact(
        &mut self,
        kind: cache::EntryKind,
        artifact: Sha256Digest,
        jid: JobId,
    ) -> GetArtifact {
        self.get_artifact(kind, artifact, jid)
    }

    fn got_artifact_failure(
        &mut self,
        kind: cache::EntryKind,
        digest: &Sha256Digest,
    ) -> Vec<JobId> {
        self.got_artifact_failure(kind, digest)
    }

    fn got_artifact_success(
        &mut self,
        kind: cache::EntryKind,
        digest: &Sha256Digest,
        artifact: GotArtifact<cache::StdFs>,
    ) -> Vec<JobId> {
        self.got_artifact_success(kind, digest, artifact)
    }

    fn decrement_ref_count(&mut self, kind: cache::EntryKind, digest: &Sha256Digest) {
        self.decrement_ref_count(kind, digest)
    }

    fn cache_path(&self, kind: cache::EntryKind, digest: &Sha256Digest) -> PathBuf {
        self.cache_path(kind, digest)
    }

    fn temp_file(&self) -> cache::StdTempFile {
        self.temp_file()
    }

    fn temp_dir(&self) -> cache::StdTempDir {
        self.temp_dir()
    }
}

/// An input message for the dispatcher. These come from the broker, an executor, or an artifact
/// fetcher.
#[derive(Debug)]
pub enum Message<FsT: cache::Fs> {
    Broker(BrokerToWorker),
    JobCompleted(JobId, JobResult<JobCompleted, String>),
    JobTimer(JobId),
    ArtifactFetchCompleted(Sha256Digest, Result<GotArtifact<FsT>>),
    BuiltBottomFsLayer(Sha256Digest, Result<GotArtifact<FsT>>),
    BuiltUpperFsLayer(Sha256Digest, Result<GotArtifact<FsT>>),
    ReadManifestDigests(Sha256Digest, JobId, Result<HashSet<Sha256Digest>>),
    Shutdown(Error),
}

impl<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
    Dispatcher<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
where
    DepsT: Deps,
    ArtifactFetcherT: ArtifactFetcher<DepsT::Fs>,
    BrokerSenderT: BrokerSender,
    CacheT: Cache<DepsT::Fs>,
{
    /// Create a new dispatcher with the provided slot count. The slot count must be a positive
    /// number.
    pub fn new(
        deps: DepsT,
        artifact_fetcher: ArtifactFetcherT,
        broker_sender: BrokerSenderT,
        cache: CacheT,
        slots: Slots,
    ) -> Self {
        Dispatcher {
            deps,
            artifact_fetcher,
            broker_sender,
            cache,
            slots: slots.into_inner().into(),
            awaiting_layers: HashMap::default(),
            available: BinaryHeap::default(),
            executing: HashMap::default(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [Message] for more information.
    pub fn receive_message(&mut self, msg: Message<DepsT::Fs>) {
        match msg {
            Message::Broker(BrokerToWorker::EnqueueJob(jid, spec)) => {
                self.receive_enqueue_job(jid, spec)
            }
            Message::Broker(BrokerToWorker::CancelJob(jid)) => self.receive_cancel_job(jid),
            Message::JobCompleted(jid, result) => self.receive_job_completed(jid, result),
            Message::JobTimer(jid) => self.receive_job_timer(jid),
            Message::ArtifactFetchCompleted(digest, Ok(artifact)) => {
                self.receive_artifact_success(digest, artifact)
            }
            Message::ArtifactFetchCompleted(digest, Err(err)) => {
                self.receive_artifact_failure(digest, err)
            }
            Message::BuiltBottomFsLayer(digest, Ok(artifact)) => {
                self.receive_build_bottom_fs_layer_success(digest, artifact)
            }
            Message::BuiltBottomFsLayer(digest, Err(err)) => {
                self.receive_build_bottom_fs_layer_failure(digest, err)
            }
            Message::BuiltUpperFsLayer(digest, Ok(artifact)) => {
                self.receive_build_upper_fs_layer_success(digest, artifact)
            }
            Message::BuiltUpperFsLayer(digest, Err(err)) => {
                self.receive_build_upper_fs_layer_failure(digest, err)
            }
            Message::ReadManifestDigests(digest, jid, Ok(digests)) => {
                self.receive_read_manifest_digests_success(digest, jid, digests)
            }
            Message::ReadManifestDigests(digest, jid, Err(err)) => {
                self.receive_read_manifest_digests_failure(digest, jid, err)
            }
            Message::Shutdown(_) => self.receive_shutdown(),
        }
    }
}

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

/// This struct represents a job where there's still work to do to fetch and/or build layers. These
/// jobs sit on the sideline until we have their layers ready. At that point, they become
/// `AvailableJob`s.
struct AwaitingLayersJob {
    spec: JobSpec,
    tracker: LayerTracker,
}

/// This struct represents a job that is ready to be executed, but isn't yet executing. These jobs
/// sit in a queue until there are slots available for them. At that point, they become
/// `ExecutingJob`s.
#[derive(Debug)]
struct AvailableJob {
    jid: JobId,
    spec: JobSpec,
    path: PathBuf,
    cache_keys: HashSet<cache::Key>,
}

impl PartialEq for AvailableJob {
    fn eq(&self, other: &Self) -> bool {
        self.spec.priority.eq(&other.spec.priority)
            && self
                .spec
                .estimated_duration
                .eq(&other.spec.estimated_duration)
    }
}

impl PartialOrd for AvailableJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for AvailableJob {}

impl Ord for AvailableJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.spec.priority.cmp(&other.spec.priority).then_with(|| {
            duration::cmp(
                &self.spec.estimated_duration,
                &other.spec.estimated_duration,
            )
        })
    }
}

/// An executing job may have been canceled or timed out, or it may be executing normally.
enum ExecutingJobState<DepsT: Deps> {
    /// The job is executing normally. It hasn't been canceled or timed-out. When it terminates,
    /// we're going to send a `JobOutcome::Completed` result to the broker, unless it times out or
    /// is canceled in the meantime.
    Nominal {
        _job_handle: DepsT::JobHandle,
        _timer_handle: Option<DepsT::TimerHandle>,
    },

    /// The job has been canceled. We've already used the job and timer handles to cancel the job
    /// and timer, respectively. Now we're waiting for it to terminate. When it terminates, we're
    /// not going to send any result to the broker.
    Canceled,

    /// The job timer has expired. We've already used the job handle to cancel the job. Now we're
    /// waiting for it to terminate. When it terminates, we're going to send a
    /// `JobOutcome::TimedOut`, unless it is canceled in the meantime. We need to wait for it to
    /// terminate before sending the `JobOutcome::TimedOut` because we want to include the
    /// `JobEffects`, which we won't get until it terminates.
    TimedOut,
}

/// This struct represents an executing job. It is created when we call `start_job` on our deps,
/// and destroyed when we get a `Message::JobCompleted`.
struct ExecutingJob<DepsT: Deps> {
    state: ExecutingJobState<DepsT>,
    cache_keys: HashSet<cache::Key>,
}

/// Manage jobs based on the slot count and requests from the broker. If the broker sends more job
/// requests than there are slots, the extra requests are queued in a FIFO queue. It's up to the
/// broker to order the requests properly.
///
/// All methods are completely nonblocking. They will never block the task or the thread.
pub struct Dispatcher<DepsT: Deps, ArtifactFetcherT, BrokerSenderT, CacheT> {
    deps: DepsT,
    artifact_fetcher: ArtifactFetcherT,
    broker_sender: BrokerSenderT,
    cache: CacheT,
    slots: usize,
    awaiting_layers: HashMap<JobId, AwaitingLayersJob>,
    available: BinaryHeap<AvailableJob>,
    executing: HashMap<JobId, ExecutingJob<DepsT>>,
}

struct Fetcher<'dispatcher, DepsT, ArtifactFetcherT, CacheT> {
    deps: &'dispatcher mut DepsT,
    artifact_fetcher: &'dispatcher mut ArtifactFetcherT,
    cache: &'dispatcher mut CacheT,
    jid: JobId,
}

impl<'dispatcher, DepsT, ArtifactFetcherT, CacheT> tracker::Fetcher
    for Fetcher<'dispatcher, DepsT, ArtifactFetcherT, CacheT>
where
    DepsT: Deps,
    ArtifactFetcherT: ArtifactFetcher<DepsT::Fs>,
    CacheT: Cache<DepsT::Fs>,
{
    fn fetch_artifact(&mut self, digest: &Sha256Digest) -> FetcherResult {
        match self
            .cache
            .get_artifact(cache::EntryKind::Blob, digest.clone(), self.jid)
        {
            GetArtifact::Success => {
                FetcherResult::Got(self.cache.cache_path(cache::EntryKind::Blob, digest))
            }
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get => {
                self.artifact_fetcher
                    .start_artifact_fetch(self.cache, digest.clone());
                FetcherResult::Pending
            }
        }
    }

    fn fetch_bottom_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        artifact_type: ArtifactType,
        artifact_path: &Path,
    ) -> FetcherResult {
        match self
            .cache
            .get_artifact(cache::EntryKind::BottomFsLayer, digest.clone(), self.jid)
        {
            GetArtifact::Success => FetcherResult::Got(
                self.cache
                    .cache_path(cache::EntryKind::BottomFsLayer, digest),
            ),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get => {
                let path = self.cache.temp_dir();
                self.deps.build_bottom_fs_layer(
                    digest.clone(),
                    path,
                    artifact_type,
                    artifact_path.into(),
                );
                FetcherResult::Pending
            }
        }
    }

    fn fetch_upper_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        lower_layer_path: &Path,
        upper_layer_path: &Path,
    ) -> FetcherResult {
        match self
            .cache
            .get_artifact(cache::EntryKind::UpperFsLayer, digest.clone(), self.jid)
        {
            GetArtifact::Success => FetcherResult::Got(
                self.cache
                    .cache_path(cache::EntryKind::UpperFsLayer, digest),
            ),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get => {
                let path = self.cache.temp_dir();
                self.deps.build_upper_fs_layer(
                    digest.clone(),
                    path,
                    lower_layer_path.into(),
                    upper_layer_path.into(),
                );
                FetcherResult::Pending
            }
        }
    }

    fn fetch_manifest_digests(&mut self, digest: &Sha256Digest, path: &Path) {
        self.deps
            .read_manifest_digests(digest.clone(), path.into(), self.jid);
    }
}

impl<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
    Dispatcher<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
where
    DepsT: Deps,
    ArtifactFetcherT: ArtifactFetcher<DepsT::Fs>,
    BrokerSenderT: BrokerSender,
    CacheT: Cache<DepsT::Fs>,
{
    /// Start at most one job, depending on whether there are any queued jobs and if there are any
    /// available slots.
    fn possibly_start_job(&mut self) -> bool {
        if self.executing.len() >= self.slots {
            return false;
        }
        let Some(AvailableJob {
            jid,
            spec,
            path,
            cache_keys,
        }) = self.available.pop()
        else {
            return false;
        };
        let timer_handle = spec
            .timeout
            .map(|timeout| self.deps.start_timer(jid, Duration::from(timeout)));
        let job_handle = self.deps.start_job(jid, spec, path);
        let executing_job = ExecutingJob {
            state: ExecutingJobState::Nominal {
                _job_handle: job_handle,
                _timer_handle: timer_handle,
            },
            cache_keys,
        };
        self.executing.insert(jid, executing_job).assert_is_none();
        self.broker_sender
            .send_message_to_broker(WorkerToBroker::JobStatusUpdate(
                jid,
                JobWorkerStatus::Executing,
            ));
        true
    }

    /// Put a job on the available jobs queue. At this point, it must have all of its artifacts.
    fn make_job_available(&mut self, jid: JobId, spec: JobSpec, tracker: LayerTracker) {
        let (path, cache_keys) = tracker.into_path_and_cache_keys();
        self.available.push(AvailableJob {
            jid,
            spec,
            path,
            cache_keys,
        });
        if !self.possibly_start_job() {
            self.broker_sender
                .send_message_to_broker(WorkerToBroker::JobStatusUpdate(
                    jid,
                    JobWorkerStatus::WaitingToExecute,
                ));
        }
    }

    fn receive_enqueue_job(&mut self, jid: JobId, spec: JobSpec) {
        let mut fetcher = Fetcher {
            deps: &mut self.deps,
            artifact_fetcher: &mut self.artifact_fetcher,
            cache: &mut self.cache,
            jid,
        };
        let tracker = LayerTracker::new(&spec.layers, &mut fetcher);
        if tracker.is_complete() {
            self.make_job_available(jid, spec, tracker);
        } else {
            self.awaiting_layers
                .insert(jid, AwaitingLayersJob { spec, tracker })
                .assert_is_none();
            self.broker_sender
                .send_message_to_broker(WorkerToBroker::JobStatusUpdate(
                    jid,
                    JobWorkerStatus::WaitingForLayers,
                ));
        }
    }

    fn receive_cancel_job(&mut self, jid: JobId) {
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // We may have already gotten some layers. Make sure we release those.
            for cache::Key { kind, digest } in entry.tracker.into_cache_keys() {
                self.cache.decrement_ref_count(kind, &digest);
            }
        } else if let Some(&mut ExecutingJob { ref mut state, .. }) = self.executing.get_mut(&jid) {
            // The job was executing. We kill the job and cancel a timer if there is one, but we
            // wait around until it's actually teriminated. We don't want to release the layers
            // until then. If we didn't it would be possible for us to try to remove a directory
            // that was still in use, which would fail.
            *state = ExecutingJobState::Canceled;
        } else {
            // It may be the queue.
            let mut keys_to_drop: Vec<cache::Key> = vec![];
            let keys_to_drop_ref = &mut keys_to_drop;
            self.available.retain(|entry| {
                if entry.jid != jid {
                    true
                } else {
                    keys_to_drop_ref.extend(entry.cache_keys.iter().map(Clone::clone));
                    false
                }
            });
            for cache::Key { kind, digest } in keys_to_drop {
                self.cache.decrement_ref_count(kind, &digest);
            }
        }
    }

    fn receive_job_completed(&mut self, jid: JobId, result: JobResult<JobCompleted, String>) {
        let Some(ExecutingJob { state, cache_keys }) = self.executing.remove(&jid) else {
            panic!("missing entry for {jid:?}");
        };

        match state {
            ExecutingJobState::Nominal { .. } => {
                self.broker_sender
                    .send_message_to_broker(WorkerToBroker::JobResponse(
                        jid,
                        result.map(JobOutcome::Completed),
                    ));
            }
            ExecutingJobState::Canceled => {}
            ExecutingJobState::TimedOut => {
                self.broker_sender
                    .send_message_to_broker(WorkerToBroker::JobResponse(
                        jid,
                        result.map(|c| JobOutcome::TimedOut(c.effects)),
                    ))
            }
        }

        for cache::Key { kind, digest } in cache_keys {
            self.cache.decrement_ref_count(kind, &digest);
        }
        self.possibly_start_job();
    }

    fn receive_job_timer(&mut self, jid: JobId) {
        let Some(&mut ExecutingJob {
            ref mut state,
            cache_keys: _,
        }) = self.executing.get_mut(&jid)
        else {
            return;
        };
        // We kill the job, but we wait around until it's actually
        // teriminated. We don't want to release the layers until the job has terminated. If we
        // didn't it would be possible for us to try to remove a directory that was still in
        // use, which would fail.
        match state {
            ExecutingJobState::Nominal { .. } => {
                *state = ExecutingJobState::TimedOut;
            }
            ExecutingJobState::TimedOut => {
                panic!("two timer expirations for job {jid:?}");
            }
            ExecutingJobState::Canceled => {}
        }
    }

    fn job_failure(&mut self, digest: &Sha256Digest, jid: JobId, msg: &str, err: &Error) {
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // If this was the first layer error for this request, then we'll find something in
            // the hash table, and we'll need to clean up.
            //
            // Otherwise, it means that there were previous errors for this entry, or it was
            // canceled, and there's nothing to do here.
            self.broker_sender
                .send_message_to_broker(WorkerToBroker::JobResponse(
                    jid,
                    Err(JobError::System(format!("{msg} {digest}: {err:?}"))),
                ));
            for cache::Key { kind, digest } in entry.tracker.into_cache_keys() {
                self.cache.decrement_ref_count(kind, &digest);
            }
        }
    }

    fn cache_fill_failure(
        &mut self,
        kind: cache::EntryKind,
        digest: Sha256Digest,
        msg: &str,
        err: Error,
    ) {
        for jid in self.cache.got_artifact_failure(kind, &digest) {
            self.job_failure(&digest, jid, msg, &err)
        }
    }

    fn advance_job(
        &mut self,
        jid: JobId,
        kind: cache::EntryKind,
        digest: &Sha256Digest,
        cb: impl FnOnce(
            &mut LayerTracker,
            &Sha256Digest,
            &mut Fetcher<'_, DepsT, ArtifactFetcherT, CacheT>,
        ),
    ) {
        match self.awaiting_layers.entry(jid) {
            Entry::Vacant(_) => {
                // If there were previous errors for this job, or the job was canceled, then
                // we'll find nothing in the hash table, and we'll need to release this layer.
                self.cache.decrement_ref_count(kind, digest);
            }
            Entry::Occupied(mut entry) => {
                // So far all is good. We then need to check if we've gotten all layers. If we
                // have, then we can go ahead and schedule the job.
                let mut fetcher = Fetcher {
                    deps: &mut self.deps,
                    artifact_fetcher: &mut self.artifact_fetcher,
                    cache: &mut self.cache,
                    jid,
                };
                cb(&mut entry.get_mut().tracker, digest, &mut fetcher);
                if entry.get().tracker.is_complete() {
                    let AwaitingLayersJob { spec, tracker } = entry.remove();
                    self.make_job_available(jid, spec, tracker);
                }
            }
        }
    }

    fn cache_fill_success(
        &mut self,
        kind: cache::EntryKind,
        digest: Sha256Digest,
        artifact: GotArtifact<DepsT::Fs>,
        cb: impl Fn(
            &mut LayerTracker,
            &Sha256Digest,
            PathBuf,
            &mut Fetcher<'_, DepsT, ArtifactFetcherT, CacheT>,
        ),
    ) {
        let jobs = self.cache.got_artifact_success(kind, &digest, artifact);
        for jid in jobs {
            let path = self.cache.cache_path(kind, &digest);
            self.advance_job(jid, kind, &digest, |tracker, digest, fetcher| {
                cb(tracker, digest, path, fetcher)
            });
        }
    }

    fn receive_artifact_success(&mut self, digest: Sha256Digest, artifact: GotArtifact<DepsT::Fs>) {
        self.cache_fill_success(
            cache::EntryKind::Blob,
            digest,
            artifact,
            |tracker, digest, path, fetcher| tracker.got_artifact(digest, path, fetcher),
        )
    }

    fn receive_artifact_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to download and extract layer artifact";
        self.cache_fill_failure(cache::EntryKind::Blob, digest, msg, err)
    }

    fn receive_build_bottom_fs_layer_success(
        &mut self,
        digest: Sha256Digest,
        artifact: GotArtifact<DepsT::Fs>,
    ) {
        self.cache_fill_success(
            cache::EntryKind::BottomFsLayer,
            digest,
            artifact,
            |tracker, digest, path, fetcher| tracker.got_bottom_fs_layer(digest, path, fetcher),
        )
    }

    fn receive_build_bottom_fs_layer_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to build bottom FS layer";
        self.cache_fill_failure(cache::EntryKind::BottomFsLayer, digest, msg, err)
    }

    fn receive_build_upper_fs_layer_success(
        &mut self,
        digest: Sha256Digest,
        artifact: GotArtifact<DepsT::Fs>,
    ) {
        self.cache_fill_success(
            cache::EntryKind::UpperFsLayer,
            digest,
            artifact,
            |tracker, digest, path, fetcher| tracker.got_upper_fs_layer(digest, path, fetcher),
        )
    }

    fn receive_build_upper_fs_layer_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to build upper FS layer";
        self.cache_fill_failure(cache::EntryKind::UpperFsLayer, digest, msg, err)
    }

    fn receive_read_manifest_digests_success(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        digests: HashSet<Sha256Digest>,
    ) {
        self.advance_job(
            jid,
            cache::EntryKind::Blob,
            &digest,
            move |tracker, digest, fetcher| {
                tracker.got_manifest_digests(digest, digests, fetcher);
            },
        );
    }

    fn receive_read_manifest_digests_failure(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        err: Error,
    ) {
        self.job_failure(&digest, jid, "failed to read manifest", &err);
    }

    /// Close our connection to the broker, drop pending work, and cancel all jobs.
    fn receive_shutdown(&mut self) {
        self.broker_sender.close();
        self.awaiting_layers = Default::default();
        self.available = Default::default();

        for jid in self.executing.keys().cloned().collect::<Vec<_>>() {
            self.receive_cancel_job(jid);
        }
    }

    /// Returns the number of executing jobs
    pub fn num_executing(&self) -> usize {
        self.executing.len()
    }
}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::{Message::*, *};
    use crate::cache::{EntryKind::*, FsTempDir, FsTempFile};
    use anyhow::anyhow;
    use maelstrom_base::{self as base, JobEffects, JobOutputResult, JobTerminationStatus};
    use maelstrom_test::*;
    use std::{cell::RefCell, rc::Rc, time::Duration};
    use BrokerToWorker::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum TestMessage {
        StartJob(JobId, JobSpec, PathBuf),
        SendMessageToBroker(WorkerToBroker),
        StartArtifactFetch(Sha256Digest),
        BuildBottomFsLayer(Sha256Digest, PathBuf, ArtifactType, PathBuf),
        BuildUpperFsLayer(Sha256Digest, PathBuf, PathBuf, PathBuf),
        ReadManifestDigests(Sha256Digest, PathBuf, JobId),
        CacheGetArtifact(cache::EntryKind, Sha256Digest, JobId),
        CacheGotArtifactSuccess(
            cache::EntryKind,
            Sha256Digest,
            GotArtifact<Rc<RefCell<TestState>>>,
        ),
        CacheGotArtifactFailure(cache::EntryKind, Sha256Digest),
        CacheDecrementRefCount(cache::EntryKind, Sha256Digest),
        CachePath(cache::EntryKind, Sha256Digest),
        JobHandleDropped(JobId),
        StartTimer(JobId, Duration),
        TimerHandleDropped(JobId),
        TempFile(PathBuf),
        TempDir(PathBuf),
    }

    use TestMessage::*;

    #[derive(Debug)]
    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<cache::Key, GetArtifact>,
        got_artifact_success_returns: HashMap<cache::Key, Vec<JobId>>,
        got_artifact_failure_returns: HashMap<cache::Key, Vec<JobId>>,
        cache_path_returns: HashMap<cache::Key, PathBuf>,
        closed: bool,
        last_random_number: u64,
    }

    struct TestHandle(TestMessage, Rc<RefCell<TestState>>);

    impl Drop for TestHandle {
        fn drop(&mut self) {
            self.1.borrow_mut().messages.push(self.0.clone());
        }
    }

    impl Deps for Rc<RefCell<TestState>> {
        type Fs = Rc<RefCell<TestState>>;

        type JobHandle = TestHandle;

        fn start_job(&mut self, jid: JobId, spec: JobSpec, path: PathBuf) -> Self::JobHandle {
            let mut mut_ref = self.borrow_mut();
            mut_ref.messages.push(StartJob(jid, spec, path));
            TestHandle(TestMessage::JobHandleDropped(jid), self.clone())
        }

        type TimerHandle = TestHandle;

        fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
            self.borrow_mut().messages.push(StartTimer(jid, duration));
            TestHandle(TestMessage::TimerHandleDropped(jid), self.clone())
        }

        fn build_bottom_fs_layer(
            &mut self,
            digest: Sha256Digest,
            layer_path: TestTempDir,
            artifact_type: ArtifactType,
            artifact_path: PathBuf,
        ) {
            self.borrow_mut().messages.push(BuildBottomFsLayer(
                digest,
                layer_path.path().to_owned(),
                artifact_type,
                artifact_path,
            ));
        }

        fn build_upper_fs_layer(
            &mut self,
            digest: Sha256Digest,
            layer_path: TestTempDir,
            lower_layer_path: PathBuf,
            upper_layer_path: PathBuf,
        ) {
            self.borrow_mut().messages.push(BuildUpperFsLayer(
                digest,
                layer_path.path().to_owned(),
                lower_layer_path,
                upper_layer_path,
            ));
        }

        fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ReadManifestDigests(digest, path, jid));
        }
    }

    impl ArtifactFetcher<Rc<RefCell<TestState>>> for Rc<RefCell<TestState>> {
        fn start_artifact_fetch(
            &mut self,
            _cache: &impl Cache<Rc<RefCell<TestState>>>,
            digest: Sha256Digest,
        ) {
            self.borrow_mut().messages.push(StartArtifactFetch(digest));
        }
    }

    impl BrokerSender for Rc<RefCell<TestState>> {
        fn send_message_to_broker(&mut self, message: WorkerToBroker) {
            let mut b = self.borrow_mut();
            if !b.closed {
                b.messages.push(SendMessageToBroker(message));
            }
        }

        fn close(&mut self) {
            let mut b = self.borrow_mut();
            b.closed = true;
        }
    }

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct TestTempFile(PathBuf);

    impl FsTempFile for TestTempFile {
        fn path(&self) -> &Path {
            &self.0
        }

        fn persist(self, _target: &Path) {
            unimplemented!()
        }
    }

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct TestTempDir(PathBuf);

    impl FsTempDir for TestTempDir {
        fn path(&self) -> &Path {
            &self.0
        }

        fn persist(self, _target: &Path) {
            unimplemented!()
        }
    }

    impl cache::Fs for Rc<RefCell<TestState>> {
        type TempFile = TestTempFile;
        type TempDir = TestTempDir;

        fn rand_u64(&self) -> u64 {
            let mut state = self.borrow_mut();
            state.last_random_number += 1;
            state.last_random_number
        }

        fn file_exists(&self, _path: &Path) -> bool {
            unimplemented!()
        }

        fn rename(&self, _source: &Path, _destination: &Path) {
            unimplemented!()
        }

        fn remove(&self, _path: &Path) {
            unimplemented!()
        }

        fn remove_recursively_on_thread(&self, _path: PathBuf) {
            unimplemented!()
        }

        fn mkdir_recursively(&self, _path: &Path) {
            unimplemented!()
        }

        fn read_dir(
            &self,
            _path: &Path,
        ) -> Box<dyn Iterator<Item = (PathBuf, cache::FileMetadata)>> {
            unimplemented!()
        }

        fn create_file(&self, _path: &Path, _content: &[u8]) {
            unimplemented!()
        }

        fn symlink(&self, _target: &Path, _link: &Path) {
            unimplemented!()
        }

        fn metadata(&self, _path: &Path) -> cache::FileMetadata {
            unimplemented!()
        }

        fn temp_file(&self, parent: &Path) -> Self::TempFile {
            let path = parent.join(format!("{:0>16x}", self.rand_u64()));
            self.borrow_mut().messages.push(TempFile(path.clone()));
            TestTempFile(path)
        }

        fn temp_dir(&self, parent: &Path) -> Self::TempDir {
            let path = parent.join(format!("{:0>16x}", self.rand_u64()));
            self.borrow_mut().messages.push(TempDir(path.clone()));
            TestTempDir(path)
        }
    }

    impl Cache<Rc<RefCell<TestState>>> for Rc<RefCell<TestState>> {
        fn get_artifact(
            &mut self,
            kind: cache::EntryKind,
            digest: Sha256Digest,
            jid: JobId,
        ) -> GetArtifact {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifact(kind, digest.clone(), jid));
            self.borrow_mut()
                .get_artifact_returns
                .remove(&cache::Key::new(kind, digest.clone()))
                .unwrap_or_else(|| panic!("unexpected get_artifact of {kind:?} {digest}"))
        }

        fn got_artifact_failure(
            &mut self,
            kind: cache::EntryKind,
            digest: &Sha256Digest,
        ) -> Vec<JobId> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactFailure(kind, digest.clone()));
            self.borrow_mut()
                .got_artifact_failure_returns
                .remove(&cache::Key::new(kind, digest.clone()))
                .unwrap()
        }

        fn got_artifact_success(
            &mut self,
            kind: cache::EntryKind,
            digest: &Sha256Digest,
            artifact: GotArtifact<Rc<RefCell<TestState>>>,
        ) -> Vec<JobId> {
            self.borrow_mut().messages.push(CacheGotArtifactSuccess(
                kind,
                digest.clone(),
                artifact,
            ));
            self.borrow_mut()
                .got_artifact_success_returns
                .remove(&cache::Key::new(kind, digest.clone()))
                .unwrap()
        }

        fn decrement_ref_count(&mut self, kind: cache::EntryKind, digest: &Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefCount(kind, digest.clone()))
        }

        fn cache_path(&self, kind: cache::EntryKind, digest: &Sha256Digest) -> PathBuf {
            self.borrow_mut()
                .messages
                .push(CachePath(kind, digest.clone()));
            self.borrow()
                .cache_path_returns
                .get(&cache::Key::new(kind, digest.clone()))
                .unwrap()
                .clone()
        }

        fn temp_file(&self) -> TestTempFile {
            TestTempFile("tmp".into())
        }

        fn temp_dir(&self) -> TestTempDir {
            TestTempDir("tmp".into())
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        dispatcher: Dispatcher<
            Rc<RefCell<TestState>>,
            Rc<RefCell<TestState>>,
            Rc<RefCell<TestState>>,
            Rc<RefCell<TestState>>,
        >,
    }

    impl Fixture {
        fn new<const L: usize, const M: usize, const N: usize, const O: usize>(
            slots: u16,
            get_artifact_returns: [(cache::Key, GetArtifact); L],
            got_artifact_success_returns: [(cache::Key, Vec<JobId>); M],
            got_artifact_failure_returns: [(cache::Key, Vec<JobId>); N],
            cache_path_returns: [(cache::Key, PathBuf); O],
        ) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
                get_artifact_returns: HashMap::from(get_artifact_returns),
                got_artifact_success_returns: HashMap::from(got_artifact_success_returns),
                got_artifact_failure_returns: HashMap::from(got_artifact_failure_returns),
                cache_path_returns: HashMap::from(cache_path_returns),
                closed: false,
                last_random_number: 0,
            }));
            let dispatcher = Dispatcher::new(
                test_state.clone(),
                test_state.clone(),
                test_state.clone(),
                test_state.clone(),
                Slots::try_from(slots).unwrap(),
            );
            Fixture {
                test_state,
                dispatcher,
            }
        }

        fn expect_messages_in_any_order(&mut self, mut expected: Vec<TestMessage>) {
            expected.sort();
            let messages = &mut self.test_state.borrow_mut().messages;
            messages.sort();
            if expected == *messages {
                messages.clear();
                return;
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n\
                Expected: {expected:#?}\n\
                Actual: {messages:#?}\n\
                Diff: {}",
                colored_diff::PrettyDifference {
                    expected: &format!("{expected:#?}"),
                    actual: &format!("{messages:#?}")
                }
            );
        }
    }

    macro_rules! upper_digest {
        ($n1:expr, $($n:expr),*) => {
            tracker::upper_layer_digest(&digest!($n1), &upper_digest!($($n),*))
        };
        ($n1:expr) => {
            digest!($n1)
        }
    }

    macro_rules! cache_key {
        (UpperFsLayer, $($d:expr),*) => {
            cache::Key::new(UpperFsLayer, upper_digest!($($d),*))
        };
        ($kind:expr, $d:expr) => {
            cache::Key::new($kind, digest!($d))
        };
    }

    macro_rules! script_test {
        ($test_name:ident, $fixture:expr, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture;
                $(
                    fixture.dispatcher.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )+
            }
        };
    }

    script_test! {
        enqueue_immediate_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success),
            (cache_key!(Blob, 42), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 41), path_buf!("/z/b/41")),
            (cache_key!(Blob, 42), path_buf!("/z/b/42")),
            (cache_key!(BottomFsLayer, 41), path_buf!("/z/bl/41")),
            (cache_key!(BottomFsLayer, 42), path_buf!("/z/bl/42")),
            (cache_key!(UpperFsLayer, 42, 41), path_buf!("/z/ul/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CachePath(Blob, digest!(41)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CachePath(BottomFsLayer, digest!(41)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CachePath(Blob, digest!(42)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CachePath(BottomFsLayer, digest!(42)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            CachePath(UpperFsLayer, upper_digest!(42, 41)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/z/ul/42/41")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
    }

    script_test! {
        enqueue_mixed_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success),
            (cache_key!(Blob, 42), GetArtifact::Get),
            (cache_key!(Blob, 43), GetArtifact::Wait),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 41), path_buf!("/z/b/41")),
            (cache_key!(Blob, 42), path_buf!("/z/b/42")),
            (cache_key!(BottomFsLayer, 41), path_buf!("/z/bl/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (43, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CachePath(Blob, digest!(41)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CachePath(BottomFsLayer, digest!(41)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(Blob, digest!(43), jid!(1)),
            StartArtifactFetch(digest!(42)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::WaitingForLayers)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
        };
    }

    script_test! {
        jobs_are_executed_in_lpt_order,
        Fixture::new(2, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(Blob, 3), GetArtifact::Success),
            (cache_key!(Blob, 4), GetArtifact::Success),
            (cache_key!(Blob, 5), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 3), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 4), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 5), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(Blob, 3), path_buf!("/z/b/3")),
            (cache_key!(Blob, 4), path_buf!("/z/b/4")),
            (cache_key!(Blob, 5), path_buf!("/z/b/5")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
            (cache_key!(BottomFsLayer, 3), path_buf!("/z/bl/3")),
            (cache_key!(BottomFsLayer, 4), path_buf!("/z/bl/4")),
            (cache_key!(BottomFsLayer, 5), path_buf!("/z/bl/5")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar).estimated_duration(Some(millis!(10))))) => {
            CacheGetArtifact(Blob, digest!(3), jid!(3)),
            CachePath(Blob, digest!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(3), jid!(3)),
            CachePath(BottomFsLayer, digest!(3)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar).estimated_duration(Some(millis!(100))))) => {
            CacheGetArtifact(Blob, digest!(4), jid!(4)),
            CachePath(Blob, digest!(4)),
            CacheGetArtifact(BottomFsLayer, digest!(4), jid!(4)),
            CachePath(BottomFsLayer, digest!(4)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(5), spec!(5, Tar))) => {
            CacheGetArtifact(Blob, digest!(5), jid!(5)),
            CachePath(Blob, digest!(5)),
            CacheGetArtifact(BottomFsLayer, digest!(5), jid!(5)),
            CachePath(BottomFsLayer, digest!(5)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::WaitingToExecute)),
        };

        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(5), spec!(5, Tar), path_buf!("/z/bl/5")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(2))) => {
            JobHandleDropped(jid!(2)),
        };
        Message::JobCompleted(jid!(2), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(2)),
            CacheDecrementRefCount(BottomFsLayer, digest!(2)),
            StartJob(jid!(4), spec!(4, Tar).estimated_duration(Some(millis!(100))), path_buf!("/z/bl/4")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(5))) => {
            JobHandleDropped(jid!(5)),
        };
        Message::JobCompleted(jid!(5), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(5)),
            CacheDecrementRefCount(BottomFsLayer, digest!(5)),
            StartJob(jid!(3), spec!(3, Tar).estimated_duration(Some(millis!(10))), path_buf!("/z/bl/3")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        jobs_are_executed_in_priority_then_lpt_order,
        Fixture::new(2, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(Blob, 3), GetArtifact::Success),
            (cache_key!(Blob, 4), GetArtifact::Success),
            (cache_key!(Blob, 5), GetArtifact::Success),
            (cache_key!(Blob, 6), GetArtifact::Success),
            (cache_key!(Blob, 7), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 3), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 4), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 5), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 6), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 7), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(Blob, 3), path_buf!("/z/b/3")),
            (cache_key!(Blob, 4), path_buf!("/z/b/4")),
            (cache_key!(Blob, 5), path_buf!("/z/b/5")),
            (cache_key!(Blob, 6), path_buf!("/z/b/6")),
            (cache_key!(Blob, 7), path_buf!("/z/b/7")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
            (cache_key!(BottomFsLayer, 3), path_buf!("/z/bl/3")),
            (cache_key!(BottomFsLayer, 4), path_buf!("/z/bl/4")),
            (cache_key!(BottomFsLayer, 5), path_buf!("/z/bl/5")),
            (cache_key!(BottomFsLayer, 6), path_buf!("/z/bl/6")),
            (cache_key!(BottomFsLayer, 7), path_buf!("/z/bl/7")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar).estimated_duration(Some(millis!(30))))) => {
            CacheGetArtifact(Blob, digest!(3), jid!(3)),
            CachePath(Blob, digest!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(3), jid!(3)),
            CachePath(BottomFsLayer, digest!(3)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar).estimated_duration(Some(millis!(40))))) => {
            CacheGetArtifact(Blob, digest!(4), jid!(4)),
            CachePath(Blob, digest!(4)),
            CacheGetArtifact(BottomFsLayer, digest!(4), jid!(4)),
            CachePath(BottomFsLayer, digest!(4)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(5), spec!(5, Tar).priority(1).estimated_duration(Some(millis!(10))))) => {
            CacheGetArtifact(Blob, digest!(5), jid!(5)),
            CachePath(Blob, digest!(5)),
            CacheGetArtifact(BottomFsLayer, digest!(5), jid!(5)),
            CachePath(BottomFsLayer, digest!(5)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(6), spec!(6, Tar).priority(1).estimated_duration(Some(millis!(20))))) => {
            CacheGetArtifact(Blob, digest!(6), jid!(6)),
            CachePath(Blob, digest!(6)),
            CacheGetArtifact(BottomFsLayer, digest!(6), jid!(6)),
            CachePath(BottomFsLayer, digest!(6)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(6), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(7), spec!(7, Tar).priority(-1).estimated_duration(Some(millis!(100))))) => {
            CacheGetArtifact(Blob, digest!(7), jid!(7)),
            CachePath(Blob, digest!(7)),
            CacheGetArtifact(BottomFsLayer, digest!(7), jid!(7)),
            CachePath(BottomFsLayer, digest!(7)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(7), JobWorkerStatus::WaitingToExecute)),
        };

        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(6), spec!(6, Tar).priority(1).estimated_duration(Some(millis!(20))), path_buf!("/z/bl/6")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(6), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(2))) => {
            JobHandleDropped(jid!(2)),
        };
        Message::JobCompleted(jid!(2), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(2)),
            CacheDecrementRefCount(BottomFsLayer, digest!(2)),
            StartJob(jid!(5), spec!(5, Tar).priority(1).estimated_duration(Some(millis!(10))), path_buf!("/z/bl/5")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(6))) => {
            JobHandleDropped(jid!(6)),
        };
        Message::JobCompleted(jid!(6), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(6)),
            CacheDecrementRefCount(BottomFsLayer, digest!(6)),
            StartJob(jid!(4), spec!(4, Tar).estimated_duration(Some(millis!(40))), path_buf!("/z/bl/4")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(5))) => {
            JobHandleDropped(jid!(5)),
        };
        Message::JobCompleted(jid!(5), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(5)),
            CacheDecrementRefCount(BottomFsLayer, digest!(5)),
            StartJob(jid!(3), spec!(3, Tar).estimated_duration(Some(millis!(30))), path_buf!("/z/bl/3")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(4))) => {
            JobHandleDropped(jid!(4)),
        };
        Message::JobCompleted(jid!(4), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(4)),
            CacheDecrementRefCount(BottomFsLayer, digest!(4)),
            StartJob(jid!(7), spec!(7, Tar).priority(-1).estimated_duration(Some(millis!(100))), path_buf!("/z/bl/7")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(7), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success),
            (cache_key!(Blob, 42), GetArtifact::Wait),
        ], [], [], [
            (cache_key!(Blob, 41), path_buf!("/z/b/41")),
            (cache_key!(BottomFsLayer, 41), path_buf!("/z/bl/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CachePath(Blob, digest!(41)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CachePath(BottomFsLayer, digest!(41)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::WaitingForLayers)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(41)),
        };
    }

    script_test! {
        cancel_executing,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success),
            (cache_key!(Blob, 42), GetArtifact::Success),
            (cache_key!(Blob, 43), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 43), GetArtifact::Success),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 41), path_buf!("/z/b/41")),
            (cache_key!(Blob, 42), path_buf!("/z/b/42")),
            (cache_key!(Blob, 43), path_buf!("/z/b/43")),
            (cache_key!(BottomFsLayer, 41), path_buf!("/z/bl/41")),
            (cache_key!(BottomFsLayer, 42), path_buf!("/z/bl/42")),
            (cache_key!(BottomFsLayer, 43), path_buf!("/z/bl/43")),
            (cache_key!(UpperFsLayer, 42, 41), path_buf!("/z/ul/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CachePath(Blob, digest!(41)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CachePath(BottomFsLayer, digest!(41)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CachePath(Blob, digest!(42)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CachePath(BottomFsLayer, digest!(42)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            CachePath(UpperFsLayer, upper_digest!(42, 41)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/z/ul/42/41")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, [(43, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(43), jid!(2)),
            CachePath(Blob, digest!(43)),
            CacheGetArtifact(BottomFsLayer, digest!(43), jid!(2)),
            CachePath(BottomFsLayer, digest!(43)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            StartJob(jid!(2), spec!(2, [(43, Tar)]), path_buf!("/z/bl/43")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        cancel_queued,
        Fixture::new(2, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(Blob, 4), GetArtifact::Success),
            (cache_key!(Blob, 41), GetArtifact::Success),
            (cache_key!(Blob, 42), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 4), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success),
            (cache_key!(UpperFsLayer, 41, 42, 41), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(Blob, 4), path_buf!("/z/b/4")),
            (cache_key!(Blob, 41), path_buf!("/z/b/41")),
            (cache_key!(Blob, 42), path_buf!("/z/b/42")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
            (cache_key!(BottomFsLayer, 4), path_buf!("/z/bl/4")),
            (cache_key!(BottomFsLayer, 41), path_buf!("/z/bl/41")),
            (cache_key!(BottomFsLayer, 42), path_buf!("/z/bl/42")),
            (cache_key!(UpperFsLayer, 42, 41), path_buf!("/z/ul/42/41")),
            (cache_key!(UpperFsLayer, 41, 42, 41), path_buf!("/z/ul/41/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, [(41, Tar), (42, Tar), (41, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(3)),
            CachePath(Blob, digest!(41)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(3)),
            CachePath(BottomFsLayer, digest!(41)),
            CacheGetArtifact(Blob, digest!(42), jid!(3)),
            CachePath(Blob, digest!(42)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(3)),
            CachePath(BottomFsLayer, digest!(42)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(3)),
            CachePath(UpperFsLayer, upper_digest!(42, 41)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(41, 42, 41), jid!(3)),
            CachePath(UpperFsLayer, upper_digest!(41, 42, 41)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(Blob, digest!(4), jid!(4)),
            CachePath(Blob, digest!(4)),
            CacheGetArtifact(BottomFsLayer, digest!(4), jid!(4)),
            CachePath(BottomFsLayer, digest!(4)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(CancelJob(jid!(3))) => {
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(41, 42, 41)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(4), spec!(4, Tar), path_buf!("/z/bl/4")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        cancel_unknown,
        Fixture::new(1, [], [], [], []),
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_canceled,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => { JobHandleDropped(jid!(1)) };
        Broker(CancelJob(jid!(1))) => {};
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_timed_out,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        JobTimer(jid!(1)) => {
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {};
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    #[test]
    fn shutdown() {
        let mut fixture = Fixture::new(
            1,
            [
                (cache_key!(Blob, 1), GetArtifact::Success),
                (cache_key!(Blob, 2), GetArtifact::Success),
                (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
                (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
            ],
            [],
            [],
            [
                (cache_key!(Blob, 1), path_buf!("/z/b/1")),
                (cache_key!(Blob, 2), path_buf!("/z/b/2")),
                (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
                (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
            ],
        );

        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), spec!(1, Tar))));
        fixture.expect_messages_in_any_order(vec![
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(
                jid!(1),
                JobWorkerStatus::Executing,
            )),
        ]);

        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(2), spec!(2, Tar))));
        fixture.expect_messages_in_any_order(vec![
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(
                jid!(2),
                JobWorkerStatus::WaitingToExecute,
            )),
        ]);

        fixture
            .dispatcher
            .receive_message(Shutdown(anyhow!("test error")));
        fixture.expect_messages_in_any_order(vec![JobHandleDropped(jid!(1))]);

        assert_eq!(fixture.dispatcher.num_executing(), 1);
        assert!(fixture.test_state.borrow().closed);

        fixture
            .dispatcher
            .receive_message(JobCompleted(jid!(1), Ok(completed!(3))));

        assert_eq!(fixture.dispatcher.num_executing(), 0);
    }

    script_test! {
        receive_ok_job_completed_executing,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        receive_error_job_completed_executing,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(Blob, 3), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 3), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(Blob, 3), path_buf!("/z/b/3")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
            (cache_key!(BottomFsLayer, 3), path_buf!("/z/bl/3")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar).estimated_duration(Some(millis!(10))))) => {
            CacheGetArtifact(Blob, digest!(3), jid!(3)),
            CachePath(Blob, digest!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(3), jid!(3)),
            CachePath(BottomFsLayer, digest!(3)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Message::JobCompleted(jid!(1), Err(JobError::System(string!("system error")))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(
                jid!(1), Err(JobError::System(string!("system error"))))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Message::JobCompleted(jid!(2), Err(JobError::Execution(string!("execution error")))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(
                jid!(2), Err(JobError::Execution(string!("execution error"))))),
            CacheDecrementRefCount(Blob, digest!(2)),
            CacheDecrementRefCount(BottomFsLayer, digest!(2)),
            JobHandleDropped(jid!(2)),
            StartJob(jid!(3), spec!(3, Tar).estimated_duration(Some(millis!(10))), path_buf!("/z/bl/3")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        receive_job_completed_canceled,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success),
            (cache_key!(Blob, 42), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 41), path_buf!("/z/b/41")),
            (cache_key!(Blob, 42), path_buf!("/z/b/42")),
            (cache_key!(BottomFsLayer, 41), path_buf!("/z/bl/41")),
            (cache_key!(BottomFsLayer, 42), path_buf!("/z/bl/41")),
            (cache_key!(UpperFsLayer, 42, 41), path_buf!("/z/ul/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CachePath(Blob, digest!(41)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CachePath(BottomFsLayer, digest!(41)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CachePath(Blob, digest!(42)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CachePath(BottomFsLayer, digest!(42)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            CachePath(UpperFsLayer, upper_digest!(42, 41)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/z/ul/42/41")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(3))) => {
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
        };
    }

    #[test]
    #[should_panic(expected = "missing entry for JobId")]
    fn receive_job_completed_unknown() {
        let mut fixture = Fixture::new(1, [], [], [], []);
        fixture
            .dispatcher
            .receive_message(Message::JobCompleted(jid!(1), Ok(completed!(1))));
    }

    script_test! {
        timer_scheduled_then_canceled_on_success,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(33)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            TimerHandleDropped(jid!(1)),
            JobHandleDropped(jid!(1)),
        };
    }

    script_test! {
        timer_scheduled_then_canceled_on_cancellation,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(33)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        };
    }

    script_test! {
        time_out_running_1,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        JobTimer(jid!(1)) => {
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobTerminationStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Inline(boxed_u8!(b"stderr")),
                duration: std::time::Duration::from_secs(1),
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(JobOutcome::TimedOut(JobEffects {
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Inline(boxed_u8!(b"stderr")),
                duration: std::time::Duration::from_secs(1),
            })))),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        time_out_completed,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            TimerHandleDropped(jid!(1)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        JobTimer(jid!(1)) => {};
    }

    script_test! {
        time_out_canceled,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(Blob, 2), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(Blob, 2), path_buf!("/z/b/2")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CachePath(Blob, digest!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            CachePath(BottomFsLayer, digest!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        };
        JobTimer(jid!(1)) => {};
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        error_cache_responses,
        Fixture::new(2, [
            (cache_key!(Blob, 41), GetArtifact::Wait),
            (cache_key!(Blob, 42), GetArtifact::Wait),
            (cache_key!(Blob, 43), GetArtifact::Wait),
            (cache_key!(Blob, 44), GetArtifact::Wait),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Wait),
            (cache_key!(BottomFsLayer, 43), GetArtifact::Wait),
        ], [
            (cache_key!(Blob, 41), vec![jid!(1)]),
            (cache_key!(Blob, 43), vec![jid!(1)]),
        ], [
            (cache_key!(Blob, 42), vec![jid!(1)]),
            (cache_key!(Blob, 44), vec![jid!(1)]),
        ], [
            (cache_key!(Blob, 41), path_buf!("/a")),
            (cache_key!(Blob, 43), path_buf!("/c")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (43, Tar), (44, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(Blob, digest!(43), jid!(1)),
            CacheGetArtifact(Blob, digest!(44), jid!(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::WaitingForLayers)),
        };
        ArtifactFetchCompleted(digest!(41), Ok(GotArtifact::File { source: TestTempFile(path_buf!("/tmp/foo"))})) => {
            CachePath(Blob, digest!(41)),
            CacheGotArtifactSuccess(Blob, digest!(41), GotArtifact::File { source: TestTempFile(path_buf!("/tmp/foo")) }),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
        };
        ArtifactFetchCompleted(digest!(42), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(Blob, digest!(42)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Err(JobError::System(
                string!("Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a: foo"))))),
            CacheDecrementRefCount(Blob, digest!(41))
        };
        ArtifactFetchCompleted(digest!(43), Ok(GotArtifact::File { source: TestTempFile(path_buf!("/tmp/bar"))})) => {
            CachePath(Blob, digest!(43)),
            CacheGotArtifactSuccess(Blob, digest!(43), GotArtifact::File { source: TestTempFile(path_buf!("/tmp/bar"))}),
            CacheDecrementRefCount(Blob, digest!(43))
        };
        ArtifactFetchCompleted(digest!(44), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(Blob, digest!(44)),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.is_none()")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(
            2,
            [
                (cache_key!(Blob, 1), GetArtifact::Success),
                (cache_key!(Blob, 2), GetArtifact::Success),
                (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
                (cache_key!(BottomFsLayer, 2), GetArtifact::Success),
            ],
            [],
            [],
            [
                (cache_key!(Blob, 1), path_buf!("/z/b/1")),
                (cache_key!(Blob, 2), path_buf!("/z/b/2")),
                (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
                (cache_key!(BottomFsLayer, 2), path_buf!("/z/bl/2")),
            ],
        );
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), spec!(1, Tar))));
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), spec!(2, Tar))));
    }

    script_test! {
        duplicate_layer_digests,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success),
            (cache_key!(UpperFsLayer, 1, 1), GetArtifact::Success),
        ], [], [], [
            (cache_key!(Blob, 1), path_buf!("/z/b/1")),
            (cache_key!(BottomFsLayer, 1), path_buf!("/z/bl/1")),
            (cache_key!(UpperFsLayer, 1, 1), path_buf!("/z/ul/1/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(1, Tar), (1, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CachePath(Blob, digest!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CachePath(BottomFsLayer, digest!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(1, 1), jid!(1)),
            CachePath(UpperFsLayer, upper_digest!(1, 1)),
            StartJob(jid!(1), spec!(1, [(1, Tar), (1, Tar)]), path_buf!("/z/ul/1/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(1, 1)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            JobHandleDropped(jid!(1)),
        };
    }
}
