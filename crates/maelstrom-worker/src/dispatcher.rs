//! Central processing module for the worker. Receive messages from the broker, executors, and
//! artifact fetchers. Start or cancel jobs as appropriate via executors.

mod tracker;

use crate::types::CacheKey;
use anyhow::{Error, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    ArtifactType, JobCompleted, JobError, JobId, JobOutcome, JobResult, JobSpec, JobWorkerStatus,
    Sha256Digest,
};
use maelstrom_util::{
    cache::{fs::Fs, GetArtifact, GotArtifact, Key as _},
    config::common::Slots,
    duration,
    ext::OptionExt as _,
};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet},
    path::{Path, PathBuf},
    result,
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

/// An input message for the dispatcher. These come from various sources.
#[derive(Debug)]
pub enum Message<FsT: Fs> {
    /// A message from the broker. These messages enqueue and cancel jobs.
    Broker(BrokerToWorker),

    /// A message notifying the dispatcher that a job has completed. The dispatcher starts jobs by
    /// calling [`Deps::start_job`], and expects each call to eventually result in one of these
    /// messages.
    JobCompleted(JobId, JobResult<JobCompleted, String>),

    /// A message notifying the dispatcher that a job has timed out. The dispatcher starts timers
    /// by calling [`Deps::start_timer`], and expects each call to eventually result in one of
    /// these messages, unless the timer is explicitly canceled by dropping the corresponding
    /// [`Deps::TimerHandle`].
    JobTimer(JobId),

    /// A message notifying the dispatcher that an artifact fetch has completed. The dispatcher
    /// starts artifact fetches by calling [`ArtifactFetcher::start_artifact_fetch`], and expects
    /// each call to eventually result in one of these messages.
    ArtifactFetchCompleted(Sha256Digest, Result<GotArtifact<FsT>>),

    /// A message notifying the dispatcher that the building of a bottom FS layer has completed.
    /// The dispatcher starts building of bottom FS layers by calling
    /// [`Deps::build_bottom_fs_layer`], and expects each call to eventually result in one of these
    /// messages.
    BuiltBottomFsLayer(Sha256Digest, Result<GotArtifact<FsT>>),

    /// A message notifying the dispatcher that the building of an upper FS layer has completed.
    /// The dispatcher starts building of upper FS layers by calling
    /// [`Deps::build_upper_fs_layer`], and expects each call to eventually result in one of these
    /// messages.
    BuiltUpperFsLayer(Sha256Digest, Result<GotArtifact<FsT>>),

    /// A message notifying the dispatcher that the reading of digests from a manifest has
    /// completed. The dispatcher starts reading digetss from a manifest by calling
    /// [`Deps::read_manifest_digests`], and expects each call to eventually result in one of these
    /// messages.
    ReadManifestDigests(Sha256Digest, JobId, Result<HashSet<Sha256Digest>>),

    /// A message notifying the dispatcher that it must enter the shutdown state. In this state,
    /// the dispatcher will schedule no new work, but will continue to process job completions. The
    /// sender can check [`Dispatcher::num_executing`] to know when all jobs have completed.
    ShutDown(Error),
}

impl<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
    Dispatcher<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
where
    DepsT: Deps,
    ArtifactFetcherT: ArtifactFetcher,
    BrokerSenderT: BrokerSender,
    CacheT: Cache,
{
    /// Create a new [`Dispatcher`] with the provided slot count. The slot count must be a positive
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
            awaiting_layers: Default::default(),
            available: Default::default(),
            executing: Default::default(),
            shut_down: false,
            shutdown_error: None,
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [`Message`] for more information.
    pub fn receive_message(&mut self, msg: Message<CacheT::Fs>) -> Result<()> {
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
            Message::ShutDown(err) => self.receive_shut_down(err),
        };
        if self.shut_down && self.executing.is_empty() {
            Err(self.shutdown_error.take().unwrap())
        } else {
            Ok(())
        }
    }
}

/// The external dependencies for [`Dispatcher`]. These methods must not block the current thread.
pub trait Deps {
    /// The job handle should kill an outstanding job when it is dropped. Even if the job is
    /// killed, the [`Dispatcher`] should always be called with [`Message::JobCompleted`]. It must
    /// be safe to drop this handle after the job has completed.
    type JobHandle;

    /// Start a new job. The dispatcher expects a [`Message::JobCompleted`] message when the job
    /// completes.
    fn start_job(&mut self, jid: JobId, spec: JobSpec, path: PathBuf) -> Self::JobHandle;

    /// The timer handle should cancel an outstanding timer when it is dropped. It must be safe to
    /// drop this handle after the timer has completed. Dropping this handle may or may not result
    /// in a [`Message::JobTimer`] message. The dispatcher must be prepared to handle the case
    /// where the message arrives even after this handle has been dropped.
    type TimerHandle;

    /// Start a timer that will send a [`Message::JobTimer`] message when it's done, unless it is
    /// canceled beforehand.
    fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle;

    /// Start a task that will build a layer-fs bottom layer out of an artifact.
    fn build_bottom_fs_layer(
        &mut self,
        digest: Sha256Digest,
        artifact_type: ArtifactType,
        artifact_path: PathBuf,
    );

    /// Start a task that will build a layer-fs upper layer by stacking a bottom layer.
    fn build_upper_fs_layer(
        &mut self,
        digest: Sha256Digest,
        lower_layer_path: PathBuf,
        upper_layer_path: PathBuf,
    );

    /// Start a task to read the digests out of the given path to a manfiest.
    fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId);
}

/// The artifact fetcher is split out of [`Deps`] for convenience. Artifact fetching is different
/// for "real" and local workers, but the rest of [`Deps`] is the same.
pub trait ArtifactFetcher {
    /// Start a thread that will download an artifact from the broker.
    fn start_artifact_fetch(&mut self, digest: Sha256Digest);
}

impl<F: FnMut(Sha256Digest) -> Result<(), E>, E> ArtifactFetcher for F {
    fn start_artifact_fetch(&mut self, digest: Sha256Digest) {
        let _ = self(digest);
    }
}

/// The broker sender is split out of [`Deps`] for convenience. Sending message to the broker is
/// different for "real" and local workers, but the rest of [`Deps`] is the same. For the "real"
/// worker, we just enqueue the message as it is, but for the local worker, we need to wrap it in a
/// local-broker-specific message.
pub trait BrokerSender {
    /// Send a message to the broker.
    fn send_message_to_broker(&mut self, message: WorkerToBroker);
}

impl<F: FnMut(WorkerToBroker) -> Result<(), E>, E> BrokerSender for F {
    fn send_message_to_broker(&mut self, message: WorkerToBroker) {
        let _ = self(message);
    }
}

/// The [`cache::Cache`] dependency for [`Dispatcher`]. This should be very similar to
/// [`cache::Cache`]'s public interface.
pub trait Cache {
    type Fs: Fs;
    fn get_artifact(&mut self, key: CacheKey, jid: JobId) -> GetArtifact;
    fn got_artifact_failure(&mut self, key: &CacheKey) -> Vec<JobId>;
    fn got_artifact_success(
        &mut self,
        key: &CacheKey,
        artifact: GotArtifact<Self::Fs>,
    ) -> result::Result<Vec<JobId>, (Error, Vec<JobId>)>;
    fn decrement_ref_count(&mut self, key: &CacheKey);
    fn cache_path(&self, key: &CacheKey) -> PathBuf;
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
    cache_keys: HashSet<CacheKey>,
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
    cache_keys: HashSet<CacheKey>,
}

/// Manage jobs based on the slot count and requests from the broker. If the broker sends more job
/// requests than there are slots, the extra requests are queued in a FIFO queue. It's up to the
/// broker to order the requests properly.
///
/// All methods are completely nonblocking. They will never block the thread.
pub struct Dispatcher<DepsT: Deps, ArtifactFetcherT, BrokerSenderT, CacheT> {
    deps: DepsT,
    artifact_fetcher: ArtifactFetcherT,
    broker_sender: BrokerSenderT,
    cache: CacheT,
    slots: usize,
    awaiting_layers: HashMap<JobId, AwaitingLayersJob>,
    available: BinaryHeap<AvailableJob>,
    executing: HashMap<JobId, ExecutingJob<DepsT>>,
    shut_down: bool,
    shutdown_error: Option<Error>,
}

impl<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
    Dispatcher<DepsT, ArtifactFetcherT, BrokerSenderT, CacheT>
where
    DepsT: Deps,
    ArtifactFetcherT: ArtifactFetcher,
    BrokerSenderT: BrokerSender,
    CacheT: Cache,
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
        if !self.shut_down {
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
    }

    fn receive_cancel_job(&mut self, jid: JobId) {
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // We may have already gotten some layers. Make sure we release those.
            for key in entry.tracker.into_cache_keys() {
                self.cache.decrement_ref_count(&key);
            }
        } else if let Some(&mut ExecutingJob { ref mut state, .. }) = self.executing.get_mut(&jid) {
            // The job was executing. We kill the job and cancel a timer if there is one, but we
            // wait around until it's actually terminated. We don't want to release the layers
            // until then. If we didn't it would be possible for us to try to remove a directory
            // that was still in use, which would fail.
            *state = ExecutingJobState::Canceled;
        } else {
            // It may be the queue.
            let mut keys_to_drop: Vec<CacheKey> = vec![];
            let keys_to_drop_ref = &mut keys_to_drop;
            self.available.retain(|entry| {
                if entry.jid != jid {
                    true
                } else {
                    keys_to_drop_ref.extend(entry.cache_keys.iter().map(Clone::clone));
                    false
                }
            });
            for key in keys_to_drop {
                self.cache.decrement_ref_count(&key);
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

        for key in cache_keys {
            self.cache.decrement_ref_count(&key);
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
        // We kill the job, but we wait around until it's actually terminated. We don't want to
        // release the layers until the job has terminated. If we didn't it would be possible for
        // us to try to remove a directory that was still in use, which would fail.
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
            for key in entry.tracker.into_cache_keys() {
                self.cache.decrement_ref_count(&key);
            }
        }
    }

    fn cache_fill_failure(&mut self, key: CacheKey, msg: &str, err: Error) {
        for jid in self.cache.got_artifact_failure(&key) {
            self.job_failure(key.digest(), jid, msg, &err)
        }
    }

    fn advance_job(
        &mut self,
        jid: JobId,
        key: &CacheKey,
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
                self.cache.decrement_ref_count(key);
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
                cb(&mut entry.get_mut().tracker, key.digest(), &mut fetcher);
                if entry.get().tracker.is_complete() {
                    let AwaitingLayersJob { spec, tracker } = entry.remove();
                    self.make_job_available(jid, spec, tracker);
                }
            }
        }
    }

    fn cache_fill_success(
        &mut self,
        key: CacheKey,
        artifact: GotArtifact<CacheT::Fs>,
        err_msg: &str,
        cb: impl Fn(
            &mut LayerTracker,
            &Sha256Digest,
            PathBuf,
            &mut Fetcher<'_, DepsT, ArtifactFetcherT, CacheT>,
        ),
    ) {
        match self.cache.got_artifact_success(&key, artifact) {
            Ok(jobs) => {
                for jid in jobs {
                    let path = self.cache.cache_path(&key);
                    self.advance_job(jid, &key, |tracker, digest, fetcher| {
                        cb(tracker, digest, path, fetcher)
                    });
                }
            }
            Err((err, jobs)) => {
                for jid in jobs {
                    self.job_failure(key.digest(), jid, err_msg, &err)
                }
            }
        }
    }

    fn receive_artifact_success(
        &mut self,
        digest: Sha256Digest,
        artifact: GotArtifact<CacheT::Fs>,
    ) {
        self.cache_fill_success(
            CacheKey::blob(digest),
            artifact,
            "Failed to save artifact in cache",
            |tracker, digest, path, fetcher| tracker.got_artifact(digest, path, fetcher),
        )
    }

    fn receive_artifact_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to download and extract layer artifact";
        self.cache_fill_failure(CacheKey::blob(digest), msg, err)
    }

    fn receive_build_bottom_fs_layer_success(
        &mut self,
        digest: Sha256Digest,
        artifact: GotArtifact<CacheT::Fs>,
    ) {
        self.cache_fill_success(
            CacheKey::bottom_fs_layer(digest),
            artifact,
            "Failed to save bottom FS layer",
            |tracker, digest, path, fetcher| tracker.got_bottom_fs_layer(digest, path, fetcher),
        )
    }

    fn receive_build_bottom_fs_layer_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to build bottom FS layer";
        self.cache_fill_failure(CacheKey::bottom_fs_layer(digest), msg, err)
    }

    fn receive_build_upper_fs_layer_success(
        &mut self,
        digest: Sha256Digest,
        artifact: GotArtifact<CacheT::Fs>,
    ) {
        self.cache_fill_success(
            CacheKey::upper_fs_layer(digest),
            artifact,
            "Failed to save upper FS layer",
            |tracker, digest, path, fetcher| tracker.got_upper_fs_layer(digest, path, fetcher),
        )
    }

    fn receive_build_upper_fs_layer_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to build upper FS layer";
        self.cache_fill_failure(CacheKey::upper_fs_layer(digest), msg, err)
    }

    fn receive_read_manifest_digests_success(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        digests: HashSet<Sha256Digest>,
    ) {
        self.advance_job(
            jid,
            &CacheKey::blob(digest),
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
    fn receive_shut_down(&mut self, shutdown_error: Error) {
        if !self.shut_down {
            self.awaiting_layers = Default::default();
            self.available = Default::default();
            self.shut_down = true;
            self.shutdown_error = Some(shutdown_error);

            for ExecutingJob { state, .. } in self.executing.values_mut() {
                // Kill all executing jobs and cancel their timers (if they have any), but wait around
                // until they are actual terminated.
                *state = ExecutingJobState::Canceled;
            }
        }
    }
}

struct Fetcher<'dispatcher, DepsT, ArtifactFetcherT, CacheT> {
    deps: &'dispatcher mut DepsT,
    artifact_fetcher: &'dispatcher mut ArtifactFetcherT,
    cache: &'dispatcher mut CacheT,
    jid: JobId,
}

impl<DepsT, ArtifactFetcherT, CacheT> tracker::Fetcher
    for Fetcher<'_, DepsT, ArtifactFetcherT, CacheT>
where
    DepsT: Deps,
    ArtifactFetcherT: ArtifactFetcher,
    CacheT: Cache,
{
    fn fetch_artifact(&mut self, digest: &Sha256Digest) -> FetcherResult {
        let key = CacheKey::blob(digest.clone());
        match self.cache.get_artifact(key.clone(), self.jid) {
            GetArtifact::Success => FetcherResult::Got(self.cache.cache_path(&key)),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get => {
                self.artifact_fetcher.start_artifact_fetch(digest.clone());
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
        let key = CacheKey::bottom_fs_layer(digest.clone());
        match self.cache.get_artifact(key.clone(), self.jid) {
            GetArtifact::Success => FetcherResult::Got(self.cache.cache_path(&key)),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get => {
                self.deps.build_bottom_fs_layer(
                    digest.clone(),
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
        let key = CacheKey::upper_fs_layer(digest.clone());
        match self.cache.get_artifact(key.clone(), self.jid) {
            GetArtifact::Success => FetcherResult::Got(self.cache.cache_path(&key)),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get => {
                self.deps.build_upper_fs_layer(
                    digest.clone(),
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
    use anyhow::anyhow;
    use maelstrom_base::{
        self as base, digest, job_spec, tar_digest, JobEffects, JobOutputResult,
        JobTerminationStatus,
    };
    use maelstrom_test::*;
    use maelstrom_util::cache::fs::test::Fs as TestFs;
    use std::{cell::RefCell, rc::Rc, time::Duration};
    use BrokerToWorker::*;

    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum TestMessage {
        StartJob(JobId, JobSpec, PathBuf),
        SendMessageToBroker(WorkerToBroker),
        StartArtifactFetch(Sha256Digest),
        BuildBottomFsLayer(Sha256Digest, ArtifactType, PathBuf),
        BuildUpperFsLayer(Sha256Digest, PathBuf, PathBuf),
        ReadManifestDigests(Sha256Digest, PathBuf, JobId),
        CacheGetArtifact(CacheKey, JobId),
        CacheGotArtifactSuccess(CacheKey, GotArtifact<TestFs>),
        CacheGotArtifactFailure(CacheKey),
        CacheDecrementRefCount(CacheKey),
        CachePath(CacheKey),
        JobHandleDropped(JobId),
        StartTimer(JobId, Duration),
        TimerHandleDropped(JobId),
    }

    use TestMessage::*;

    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<CacheKey, GetArtifact>,
        got_artifact_success_returns: HashMap<CacheKey, Vec<JobId>>,
        got_artifact_failure_returns: HashMap<CacheKey, Vec<JobId>>,
        cache_path_returns: HashMap<CacheKey, PathBuf>,
    }

    struct TestHandle(Option<TestMessage>, Rc<RefCell<TestState>>);

    impl Drop for TestHandle {
        fn drop(&mut self) {
            self.1.borrow_mut().messages.push(self.0.take().unwrap());
        }
    }

    impl Deps for Rc<RefCell<TestState>> {
        type JobHandle = TestHandle;

        fn start_job(&mut self, jid: JobId, spec: JobSpec, path: PathBuf) -> Self::JobHandle {
            let mut mut_ref = self.borrow_mut();
            mut_ref.messages.push(StartJob(jid, spec, path));
            TestHandle(Some(TestMessage::JobHandleDropped(jid)), self.clone())
        }

        type TimerHandle = TestHandle;

        fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
            self.borrow_mut().messages.push(StartTimer(jid, duration));
            TestHandle(Some(TestMessage::TimerHandleDropped(jid)), self.clone())
        }

        fn build_bottom_fs_layer(
            &mut self,
            digest: Sha256Digest,
            artifact_type: ArtifactType,
            artifact_path: PathBuf,
        ) {
            self.borrow_mut().messages.push(BuildBottomFsLayer(
                digest,
                artifact_type,
                artifact_path,
            ));
        }

        fn build_upper_fs_layer(
            &mut self,
            digest: Sha256Digest,
            lower_layer_path: PathBuf,
            upper_layer_path: PathBuf,
        ) {
            self.borrow_mut().messages.push(BuildUpperFsLayer(
                digest,
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

    impl ArtifactFetcher for Rc<RefCell<TestState>> {
        fn start_artifact_fetch(&mut self, digest: Sha256Digest) {
            self.borrow_mut().messages.push(StartArtifactFetch(digest));
        }
    }

    impl BrokerSender for Rc<RefCell<TestState>> {
        fn send_message_to_broker(&mut self, message: WorkerToBroker) {
            self.borrow_mut()
                .messages
                .push(SendMessageToBroker(message));
        }
    }

    impl Cache for Rc<RefCell<TestState>> {
        type Fs = TestFs;

        fn get_artifact(&mut self, key: CacheKey, jid: JobId) -> GetArtifact {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifact(key.clone(), jid));
            self.borrow_mut()
                .get_artifact_returns
                .remove(&key)
                .unwrap_or_else(|| panic!("unexpected get_artifact of {key:?}"))
        }

        fn got_artifact_failure(&mut self, key: &CacheKey) -> Vec<JobId> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactFailure(key.clone()));
            self.borrow_mut()
                .got_artifact_failure_returns
                .remove(key)
                .unwrap_or_else(|| panic!("unexpected got_artifact_failure of {key:?}"))
        }

        fn got_artifact_success(
            &mut self,
            key: &CacheKey,
            artifact: GotArtifact<Self::Fs>,
        ) -> result::Result<Vec<JobId>, (Error, Vec<JobId>)> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactSuccess(key.clone(), artifact));
            Ok(self
                .borrow_mut()
                .got_artifact_success_returns
                .remove(key)
                .unwrap_or_else(|| panic!("unexpected got_artifact_success of {key:?}")))
        }

        fn decrement_ref_count(&mut self, key: &CacheKey) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefCount(key.clone()))
        }

        fn cache_path(&self, key: &CacheKey) -> PathBuf {
            self.borrow_mut().messages.push(CachePath(key.clone()));
            self.borrow()
                .cache_path_returns
                .get(key)
                .unwrap_or_else(|| panic!("unexpected cache_path of {key:?}"))
                .clone()
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        #[allow(clippy::type_complexity)]
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
            get_artifact_returns: [(CacheKey, GetArtifact); L],
            got_artifact_success_returns: [(CacheKey, Vec<JobId>); M],
            got_artifact_failure_returns: [(CacheKey, Vec<JobId>); N],
            cache_path_returns: [(CacheKey, PathBuf); O],
        ) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
                get_artifact_returns: HashMap::from(get_artifact_returns),
                got_artifact_success_returns: HashMap::from(got_artifact_success_returns),
                got_artifact_failure_returns: HashMap::from(got_artifact_failure_returns),
                cache_path_returns: HashMap::from(cache_path_returns),
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

        #[track_caller]
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

        fn receive_message(&mut self, message: Message<TestFs>) {
            self.dispatcher.receive_message(message).unwrap();
        }

        fn receive_message_error(&mut self, message: Message<TestFs>, err: &str) {
            assert_eq!(
                self.dispatcher
                    .receive_message(message)
                    .unwrap_err()
                    .to_string(),
                err
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

    macro_rules! blob {
        ($digest:expr) => {
            CacheKey::blob(digest!($digest))
        };
    }

    macro_rules! bottom_fs_layer {
        ($digest:expr) => {
            CacheKey::bottom_fs_layer(digest!($digest))
        };
    }

    macro_rules! upper_fs_layer {
        ($($digest:expr),*) => {
            CacheKey::upper_fs_layer(upper_digest!($($digest),*))
        }
    }

    macro_rules! script_test {
        ($test_name:ident, $fixture:expr, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture;
                $(
                    fixture.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )+
            }
        };
    }

    script_test! {
        enqueue_immediate_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (blob!(41), GetArtifact::Success),
            (blob!(42), GetArtifact::Success),
            (bottom_fs_layer!(41), GetArtifact::Success),
            (bottom_fs_layer!(42), GetArtifact::Success),
            (upper_fs_layer!(42, 41), GetArtifact::Success),
        ], [], [], [
            (blob!(41), path_buf!("/z/b/41")),
            (blob!(42), path_buf!("/z/b/42")),
            (bottom_fs_layer!(41), path_buf!("/z/bl/41")),
            (bottom_fs_layer!(42), path_buf!("/z/bl/42")),
            (upper_fs_layer!(42, 41), path_buf!("/z/ul/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]))) => {
            CacheGetArtifact(blob!(41), jid!(1)),
            CachePath(blob!(41)),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(1)),
            CachePath(bottom_fs_layer!(41)),
            CacheGetArtifact(blob!(42), jid!(1)),
            CachePath(blob!(42)),
            CacheGetArtifact(bottom_fs_layer!(42), jid!(1)),
            CachePath(bottom_fs_layer!(42)),
            CacheGetArtifact(upper_fs_layer!(42, 41), jid!(1)),
            CachePath(upper_fs_layer!(42, 41)),
            StartJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]), path_buf!("/z/ul/42/41")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
    }

    script_test! {
        enqueue_mixed_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (blob!(41), GetArtifact::Success),
            (blob!(42), GetArtifact::Get),
            (blob!(43), GetArtifact::Wait),
            (bottom_fs_layer!(41), GetArtifact::Success),
        ], [], [], [
            (blob!(41), path_buf!("/z/b/41")),
            (blob!(42), path_buf!("/z/b/42")),
            (bottom_fs_layer!(41), path_buf!("/z/bl/41")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42), tar_digest!(43)]))) => {
            CacheGetArtifact(blob!(41), jid!(1)),
            CachePath(blob!(41)),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(1)),
            CachePath(bottom_fs_layer!(41)),
            CacheGetArtifact(blob!(42), jid!(1)),
            CacheGetArtifact(blob!(43), jid!(1)),
            StartArtifactFetch(digest!(42)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::WaitingForLayers)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(blob!(41)),
            CacheDecrementRefCount(bottom_fs_layer!(41)),
        };
    }

    script_test! {
        jobs_are_executed_in_lpt_order,
        Fixture::new(2, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (blob!(3), GetArtifact::Success),
            (blob!(4), GetArtifact::Success),
            (blob!(5), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
            (bottom_fs_layer!(3), GetArtifact::Success),
            (bottom_fs_layer!(4), GetArtifact::Success),
            (bottom_fs_layer!(5), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (blob!(3), path_buf!("/z/b/3")),
            (blob!(4), path_buf!("/z/b/4")),
            (blob!(5), path_buf!("/z/b/5")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            (bottom_fs_layer!(3), path_buf!("/z/bl/3")),
            (bottom_fs_layer!(4), path_buf!("/z/bl/4")),
            (bottom_fs_layer!(5), path_buf!("/z/bl/5")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, estimated_duration: millis!(10)))) => {
            CacheGetArtifact(blob!(3), jid!(3)),
            CachePath(blob!(3)),
            CacheGetArtifact(bottom_fs_layer!(3), jid!(3)),
            CachePath(bottom_fs_layer!(3)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, estimated_duration: millis!(100)))) => {
            CacheGetArtifact(blob!(4), jid!(4)),
            CachePath(blob!(4)),
            CacheGetArtifact(bottom_fs_layer!(4), jid!(4)),
            CachePath(bottom_fs_layer!(4)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(5), spec!(5))) => {
            CacheGetArtifact(blob!(5), jid!(5)),
            CachePath(blob!(5)),
            CacheGetArtifact(bottom_fs_layer!(5), jid!(5)),
            CachePath(bottom_fs_layer!(5)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::WaitingToExecute)),
        };

        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            StartJob(jid!(5), spec!(5), path_buf!("/z/bl/5")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(2))) => {
            JobHandleDropped(jid!(2)),
        };
        Message::JobCompleted(jid!(2), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(2)),
            CacheDecrementRefCount(bottom_fs_layer!(2)),
            StartJob(jid!(4), spec!(4, estimated_duration: millis!(100)), path_buf!("/z/bl/4")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(5))) => {
            JobHandleDropped(jid!(5)),
        };
        Message::JobCompleted(jid!(5), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(5)),
            CacheDecrementRefCount(bottom_fs_layer!(5)),
            StartJob(jid!(3), spec!(3, estimated_duration: millis!(10)), path_buf!("/z/bl/3")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        jobs_are_executed_in_priority_then_lpt_order,
        Fixture::new(2, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (blob!(3), GetArtifact::Success),
            (blob!(4), GetArtifact::Success),
            (blob!(5), GetArtifact::Success),
            (blob!(6), GetArtifact::Success),
            (blob!(7), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
            (bottom_fs_layer!(3), GetArtifact::Success),
            (bottom_fs_layer!(4), GetArtifact::Success),
            (bottom_fs_layer!(5), GetArtifact::Success),
            (bottom_fs_layer!(6), GetArtifact::Success),
            (bottom_fs_layer!(7), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (blob!(3), path_buf!("/z/b/3")),
            (blob!(4), path_buf!("/z/b/4")),
            (blob!(5), path_buf!("/z/b/5")),
            (blob!(6), path_buf!("/z/b/6")),
            (blob!(7), path_buf!("/z/b/7")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            (bottom_fs_layer!(3), path_buf!("/z/bl/3")),
            (bottom_fs_layer!(4), path_buf!("/z/bl/4")),
            (bottom_fs_layer!(5), path_buf!("/z/bl/5")),
            (bottom_fs_layer!(6), path_buf!("/z/bl/6")),
            (bottom_fs_layer!(7), path_buf!("/z/bl/7")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, estimated_duration: millis!(30)))) => {
            CacheGetArtifact(blob!(3), jid!(3)),
            CachePath(blob!(3)),
            CacheGetArtifact(bottom_fs_layer!(3), jid!(3)),
            CachePath(bottom_fs_layer!(3)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, estimated_duration: millis!(40)))) => {
            CacheGetArtifact(blob!(4), jid!(4)),
            CachePath(blob!(4)),
            CacheGetArtifact(bottom_fs_layer!(4), jid!(4)),
            CachePath(bottom_fs_layer!(4)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(5), spec!(5, priority: 1, estimated_duration: millis!(10)))) => {
            CacheGetArtifact(blob!(5), jid!(5)),
            CachePath(blob!(5)),
            CacheGetArtifact(bottom_fs_layer!(5), jid!(5)),
            CachePath(bottom_fs_layer!(5)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(6), spec!(6, priority: 1, estimated_duration: millis!(20)))) => {
            CacheGetArtifact(blob!(6), jid!(6)),
            CachePath(blob!(6)),
            CacheGetArtifact(bottom_fs_layer!(6), jid!(6)),
            CachePath(bottom_fs_layer!(6)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(6), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(7), spec!(7, priority: -1, estimated_duration: millis!(100)))) => {
            CacheGetArtifact(blob!(7), jid!(7)),
            CachePath(blob!(7)),
            CacheGetArtifact(bottom_fs_layer!(7), jid!(7)),
            CachePath(bottom_fs_layer!(7)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(7), JobWorkerStatus::WaitingToExecute)),
        };

        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            StartJob(jid!(6), spec!(6, priority: 1, estimated_duration: millis!(20)), path_buf!("/z/bl/6")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(6), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(2))) => {
            JobHandleDropped(jid!(2)),
        };
        Message::JobCompleted(jid!(2), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(2)),
            CacheDecrementRefCount(bottom_fs_layer!(2)),
            StartJob(jid!(5), spec!(5, priority: 1, estimated_duration: millis!(10)), path_buf!("/z/bl/5")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(5), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(6))) => {
            JobHandleDropped(jid!(6)),
        };
        Message::JobCompleted(jid!(6), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(6)),
            CacheDecrementRefCount(bottom_fs_layer!(6)),
            StartJob(jid!(4), spec!(4, estimated_duration: millis!(40)), path_buf!("/z/bl/4")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(5))) => {
            JobHandleDropped(jid!(5)),
        };
        Message::JobCompleted(jid!(5), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(5)),
            CacheDecrementRefCount(bottom_fs_layer!(5)),
            StartJob(jid!(3), spec!(3, estimated_duration: millis!(30)), path_buf!("/z/bl/3")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::Executing)),
        };

        Broker(CancelJob(jid!(4))) => {
            JobHandleDropped(jid!(4)),
        };
        Message::JobCompleted(jid!(4), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(4)),
            CacheDecrementRefCount(bottom_fs_layer!(4)),
            StartJob(jid!(7), spec!(7, priority: -1, estimated_duration: millis!(100)), path_buf!("/z/bl/7")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(7), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(1, [
            (blob!(41), GetArtifact::Success),
            (bottom_fs_layer!(41), GetArtifact::Success),
            (blob!(42), GetArtifact::Wait),
        ], [], [], [
            (blob!(41), path_buf!("/z/b/41")),
            (bottom_fs_layer!(41), path_buf!("/z/bl/41")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]))) => {
            CacheGetArtifact(blob!(41), jid!(1)),
            CachePath(blob!(41)),
            CacheGetArtifact(blob!(42), jid!(1)),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(1)),
            CachePath(bottom_fs_layer!(41)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::WaitingForLayers)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(bottom_fs_layer!(41)),
            CacheDecrementRefCount(blob!(41)),
        };
    }

    script_test! {
        cancel_executing,
        Fixture::new(1, [
            (blob!(41), GetArtifact::Success),
            (blob!(42), GetArtifact::Success),
            (blob!(43), GetArtifact::Success),
            (bottom_fs_layer!(41), GetArtifact::Success),
            (bottom_fs_layer!(42), GetArtifact::Success),
            (bottom_fs_layer!(43), GetArtifact::Success),
            (upper_fs_layer!(42, 41), GetArtifact::Success),
        ], [], [], [
            (blob!(41), path_buf!("/z/b/41")),
            (blob!(42), path_buf!("/z/b/42")),
            (blob!(43), path_buf!("/z/b/43")),
            (bottom_fs_layer!(41), path_buf!("/z/bl/41")),
            (bottom_fs_layer!(42), path_buf!("/z/bl/42")),
            (bottom_fs_layer!(43), path_buf!("/z/bl/43")),
            (upper_fs_layer!(42, 41), path_buf!("/z/ul/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]))) => {
            CacheGetArtifact(blob!(41), jid!(1)),
            CachePath(blob!(41)),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(1)),
            CachePath(bottom_fs_layer!(41)),
            CacheGetArtifact(blob!(42), jid!(1)),
            CachePath(blob!(42)),
            CacheGetArtifact(bottom_fs_layer!(42), jid!(1)),
            CachePath(bottom_fs_layer!(42)),
            CacheGetArtifact(upper_fs_layer!(42, 41), jid!(1)),
            CachePath(upper_fs_layer!(42, 41)),
            StartJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]), path_buf!("/z/ul/42/41")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), job_spec!("2", [tar_digest!(43)]))) => {
            CacheGetArtifact(blob!(43), jid!(2)),
            CachePath(blob!(43)),
            CacheGetArtifact(bottom_fs_layer!(43), jid!(2)),
            CachePath(bottom_fs_layer!(43)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(bottom_fs_layer!(41)),
            CacheDecrementRefCount(bottom_fs_layer!(42)),
            CacheDecrementRefCount(upper_fs_layer!(42, 41)),
            CacheDecrementRefCount(blob!(41)),
            CacheDecrementRefCount(blob!(42)),
            StartJob(jid!(2), job_spec!("2", [tar_digest!(43)]), path_buf!("/z/bl/43")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        cancel_queued,
        Fixture::new(2, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (blob!(4), GetArtifact::Success),
            (blob!(41), GetArtifact::Success),
            (blob!(42), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
            (bottom_fs_layer!(4), GetArtifact::Success),
            (bottom_fs_layer!(41), GetArtifact::Success),
            (bottom_fs_layer!(42), GetArtifact::Success),
            (upper_fs_layer!(42, 41), GetArtifact::Success),
            (upper_fs_layer!(41, 42, 41), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (blob!(4), path_buf!("/z/b/4")),
            (blob!(41), path_buf!("/z/b/41")),
            (blob!(42), path_buf!("/z/b/42")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            (bottom_fs_layer!(4), path_buf!("/z/bl/4")),
            (bottom_fs_layer!(41), path_buf!("/z/bl/41")),
            (bottom_fs_layer!(42), path_buf!("/z/bl/42")),
            (upper_fs_layer!(42, 41), path_buf!("/z/ul/42/41")),
            (upper_fs_layer!(41, 42, 41), path_buf!("/z/ul/41/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(3), job_spec!("3", [tar_digest!(41), tar_digest!(42), tar_digest!(41)]))) => {
            CacheGetArtifact(blob!(41), jid!(3)),
            CachePath(blob!(41)),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(3)),
            CachePath(bottom_fs_layer!(41)),
            CacheGetArtifact(blob!(42), jid!(3)),
            CachePath(blob!(42)),
            CacheGetArtifact(bottom_fs_layer!(42), jid!(3)),
            CachePath(bottom_fs_layer!(42)),
            CacheGetArtifact(upper_fs_layer!(42, 41), jid!(3)),
            CachePath(upper_fs_layer!(42, 41)),
            CacheGetArtifact(upper_fs_layer!(41, 42, 41), jid!(3)),
            CachePath(upper_fs_layer!(41, 42, 41)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4))) => {
            CacheGetArtifact(blob!(4), jid!(4)),
            CachePath(blob!(4)),
            CacheGetArtifact(bottom_fs_layer!(4), jid!(4)),
            CachePath(bottom_fs_layer!(4)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(4), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(CancelJob(jid!(3))) => {
            CacheDecrementRefCount(blob!(41)),
            CacheDecrementRefCount(bottom_fs_layer!(41)),
            CacheDecrementRefCount(blob!(42)),
            CacheDecrementRefCount(bottom_fs_layer!(42)),
            CacheDecrementRefCount(upper_fs_layer!(42, 41)),
            CacheDecrementRefCount(upper_fs_layer!(41, 42, 41)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(4), spec!(4), path_buf!("/z/bl/4")),
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
            (blob!(1), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => { JobHandleDropped(jid!(1)) };
        Broker(CancelJob(jid!(1))) => {};
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_timed_out,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, timeout: 1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 1), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        JobTimer(jid!(1)) => {
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {};
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    #[test]
    fn shut_down() {
        let mut fixture = Fixture::new(
            1,
            [
                (blob!(1), GetArtifact::Success),
                (blob!(2), GetArtifact::Success),
                (bottom_fs_layer!(1), GetArtifact::Success),
                (bottom_fs_layer!(2), GetArtifact::Success),
            ],
            [],
            [],
            [
                (blob!(1), path_buf!("/z/b/1")),
                (blob!(2), path_buf!("/z/b/2")),
                (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
                (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            ],
        );

        fixture.receive_message(Broker(EnqueueJob(jid!(1), spec!(1))));
        fixture.expect_messages_in_any_order(vec![
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(
                jid!(1),
                JobWorkerStatus::Executing,
            )),
        ]);

        fixture.receive_message(Broker(EnqueueJob(jid!(2), spec!(2))));
        fixture.expect_messages_in_any_order(vec![
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(
                jid!(2),
                JobWorkerStatus::WaitingToExecute,
            )),
        ]);

        fixture.receive_message(ShutDown(anyhow!("test error")));
        fixture.expect_messages_in_any_order(vec![JobHandleDropped(jid!(1))]);

        fixture.receive_message_error(JobCompleted(jid!(1), Ok(completed!(3))), "test error");
    }

    #[test]
    fn enqueue_job_is_ignored_after_shut_down() {
        let mut fixture = Fixture::new(
            2,
            [
                (blob!(1), GetArtifact::Success),
                (blob!(2), GetArtifact::Success),
                (bottom_fs_layer!(1), GetArtifact::Success),
                (bottom_fs_layer!(2), GetArtifact::Success),
            ],
            [],
            [],
            [
                (blob!(1), path_buf!("/z/b/1")),
                (blob!(2), path_buf!("/z/b/2")),
                (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
                (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            ],
        );

        fixture.receive_message(Broker(EnqueueJob(jid!(1), spec!(1, timeout: 1))));
        fixture.expect_messages_in_any_order(vec![
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 1), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(
                jid!(1),
                JobWorkerStatus::Executing,
            )),
        ]);

        fixture.receive_message(ShutDown(anyhow!("test error")));
        fixture.expect_messages_in_any_order(vec![
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        ]);

        fixture.receive_message(Broker(EnqueueJob(jid!(2), spec!(2))));
        fixture.expect_messages_in_any_order(vec![]);
    }

    script_test! {
        receive_ok_job_completed_executing,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        receive_error_job_completed_executing,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (blob!(3), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
            (bottom_fs_layer!(3), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (blob!(3), path_buf!("/z/b/3")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            (bottom_fs_layer!(3), path_buf!("/z/bl/3")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1), path_buf!("/z/bl/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, estimated_duration: millis!(10)))) => {
            CacheGetArtifact(blob!(3), jid!(3)),
            CachePath(blob!(3)),
            CacheGetArtifact(bottom_fs_layer!(3), jid!(3)),
            CachePath(bottom_fs_layer!(3)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::WaitingToExecute)),
        };
        Message::JobCompleted(jid!(1), Err(JobError::System(string!("system error")))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(
                jid!(1), Err(JobError::System(string!("system error"))))),
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        Message::JobCompleted(jid!(2), Err(JobError::Execution(string!("execution error")))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(
                jid!(2), Err(JobError::Execution(string!("execution error"))))),
            CacheDecrementRefCount(blob!(2)),
            CacheDecrementRefCount(bottom_fs_layer!(2)),
            JobHandleDropped(jid!(2)),
            StartJob(jid!(3), spec!(3, estimated_duration: millis!(10)), path_buf!("/z/bl/3")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(3), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        receive_job_completed_canceled,
        Fixture::new(1, [
            (blob!(41), GetArtifact::Success),
            (blob!(42), GetArtifact::Success),
            (bottom_fs_layer!(41), GetArtifact::Success),
            (bottom_fs_layer!(42), GetArtifact::Success),
            (upper_fs_layer!(42, 41), GetArtifact::Success),
        ], [], [], [
            (blob!(41), path_buf!("/z/b/41")),
            (blob!(42), path_buf!("/z/b/42")),
            (bottom_fs_layer!(41), path_buf!("/z/bl/41")),
            (bottom_fs_layer!(42), path_buf!("/z/bl/41")),
            (upper_fs_layer!(42, 41), path_buf!("/z/ul/42/41")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]))) => {
            CacheGetArtifact(blob!(41), jid!(1)),
            CachePath(blob!(41)),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(1)),
            CachePath(bottom_fs_layer!(41)),
            CacheGetArtifact(blob!(42), jid!(1)),
            CachePath(blob!(42)),
            CacheGetArtifact(bottom_fs_layer!(42), jid!(1)),
            CachePath(bottom_fs_layer!(42)),
            CacheGetArtifact(upper_fs_layer!(42, 41), jid!(1)),
            CachePath(upper_fs_layer!(42, 41)),
            StartJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42)]), path_buf!("/z/ul/42/41")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(3))) => {
            CacheDecrementRefCount(blob!(41)),
            CacheDecrementRefCount(bottom_fs_layer!(41)),
            CacheDecrementRefCount(blob!(42)),
            CacheDecrementRefCount(bottom_fs_layer!(42)),
            CacheDecrementRefCount(upper_fs_layer!(42, 41)),
        };
    }

    #[test]
    #[should_panic(expected = "missing entry for JobId")]
    fn receive_job_completed_unknown() {
        let mut fixture = Fixture::new(1, [], [], [], []);
        fixture.receive_message(Message::JobCompleted(jid!(1), Ok(completed!(1))));
    }

    script_test! {
        timer_scheduled_then_canceled_on_success,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, timeout: 33))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 33), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(33)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            TimerHandleDropped(jid!(1)),
            JobHandleDropped(jid!(1)),
        };
    }

    script_test! {
        timer_scheduled_then_canceled_on_cancellation,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, timeout: 33))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 33), path_buf!("/z/bl/1")),
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
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, timeout: 1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 1), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
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
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(JobOutcome::TimedOut(JobEffects {
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Inline(boxed_u8!(b"stderr")),
                duration: std::time::Duration::from_secs(1),
            })))),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        time_out_completed,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, timeout: 1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 1), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            TimerHandleDropped(jid!(1)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            JobHandleDropped(jid!(1)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
        JobTimer(jid!(1)) => {};
    }

    script_test! {
        time_out_canceled,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (blob!(2), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (bottom_fs_layer!(2), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (blob!(2), path_buf!("/z/b/2")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, timeout: 1))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            StartJob(jid!(1), spec!(1, timeout: 1), path_buf!("/z/bl/1")),
            StartTimer(jid!(1), Duration::from_secs(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Broker(EnqueueJob(jid!(2), spec!(2))) => {
            CacheGetArtifact(blob!(2), jid!(2)),
            CachePath(blob!(2)),
            CacheGetArtifact(bottom_fs_layer!(2), jid!(2)),
            CachePath(bottom_fs_layer!(2)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::WaitingToExecute)),
        };
        Broker(CancelJob(jid!(1))) => {
            JobHandleDropped(jid!(1)),
            TimerHandleDropped(jid!(1)),
        };
        JobTimer(jid!(1)) => {};
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            StartJob(jid!(2), spec!(2), path_buf!("/z/bl/2")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(2), JobWorkerStatus::Executing)),
        };
    }

    script_test! {
        error_cache_responses,
        Fixture::new(2, [
            (blob!(41), GetArtifact::Wait),
            (blob!(42), GetArtifact::Wait),
            (blob!(43), GetArtifact::Wait),
            (blob!(44), GetArtifact::Wait),
            (bottom_fs_layer!(41), GetArtifact::Wait),
            (bottom_fs_layer!(43), GetArtifact::Wait),
        ], [
            (blob!(41), vec![jid!(1)]),
            (blob!(43), vec![jid!(1)]),
        ], [
            (blob!(42), vec![jid!(1)]),
            (blob!(44), vec![jid!(1)]),
        ], [
            (blob!(41), path_buf!("/a")),
            (blob!(43), path_buf!("/c")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(41), tar_digest!(42), tar_digest!(43), tar_digest!(44)]))) => {
            CacheGetArtifact(blob!(41), jid!(1)),
            CacheGetArtifact(blob!(42), jid!(1)),
            CacheGetArtifact(blob!(43), jid!(1)),
            CacheGetArtifact(blob!(44), jid!(1)),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::WaitingForLayers)),
        };
        ArtifactFetchCompleted(digest!(41), Ok(GotArtifact::file("/tmp/foo".into()))) => {
            CachePath(blob!(41)),
            CacheGotArtifactSuccess(blob!(41), GotArtifact::file("/tmp/foo".into())),
            CacheGetArtifact(bottom_fs_layer!(41), jid!(1)),
        };
        ArtifactFetchCompleted(digest!(42), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(blob!(42)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Err(JobError::System(
                string!("Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a: foo"))))),
            CacheDecrementRefCount(blob!(41))
        };
        ArtifactFetchCompleted(digest!(43), Ok(GotArtifact::file("/tmp/bar".into()))) => {
            CachePath(blob!(43)),
            CacheGotArtifactSuccess(blob!(43), GotArtifact::file("/tmp/bar".into())),
            CacheDecrementRefCount(blob!(43))
        };
        ArtifactFetchCompleted(digest!(44), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(blob!(44)),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.is_none()")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(
            2,
            [
                (blob!(1), GetArtifact::Success),
                (blob!(2), GetArtifact::Success),
                (bottom_fs_layer!(1), GetArtifact::Success),
                (bottom_fs_layer!(2), GetArtifact::Success),
            ],
            [],
            [],
            [
                (blob!(1), path_buf!("/z/b/1")),
                (blob!(2), path_buf!("/z/b/2")),
                (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
                (bottom_fs_layer!(2), path_buf!("/z/bl/2")),
            ],
        );
        fixture.receive_message(Broker(EnqueueJob(jid!(1), spec!(1))));
        fixture.receive_message(Broker(EnqueueJob(jid!(1), spec!(2))));
    }

    script_test! {
        duplicate_layer_digests,
        Fixture::new(1, [
            (blob!(1), GetArtifact::Success),
            (bottom_fs_layer!(1), GetArtifact::Success),
            (upper_fs_layer!(1, 1), GetArtifact::Success),
        ], [], [], [
            (blob!(1), path_buf!("/z/b/1")),
            (bottom_fs_layer!(1), path_buf!("/z/bl/1")),
            (upper_fs_layer!(1, 1), path_buf!("/z/ul/1/1")),
        ]),
        Broker(EnqueueJob(jid!(1), job_spec!("1", [tar_digest!(1), tar_digest!(1)]))) => {
            CacheGetArtifact(blob!(1), jid!(1)),
            CachePath(blob!(1)),
            CacheGetArtifact(bottom_fs_layer!(1), jid!(1)),
            CachePath(bottom_fs_layer!(1)),
            CacheGetArtifact(upper_fs_layer!(1, 1), jid!(1)),
            CachePath(upper_fs_layer!(1, 1)),
            StartJob(jid!(1), job_spec!("1", [tar_digest!(1), tar_digest!(1)]), path_buf!("/z/ul/1/1")),
            SendMessageToBroker(WorkerToBroker::JobStatusUpdate(jid!(1), JobWorkerStatus::Executing)),
        };
        Message::JobCompleted(jid!(1), Ok(completed!(1))) => {
            CacheDecrementRefCount(blob!(1)),
            CacheDecrementRefCount(bottom_fs_layer!(1)),
            CacheDecrementRefCount(upper_fs_layer!(1, 1)),
            SendMessageToBroker(WorkerToBroker::JobResponse(jid!(1), Ok(outcome!(1)))),
            JobHandleDropped(jid!(1)),
        };
    }
}
