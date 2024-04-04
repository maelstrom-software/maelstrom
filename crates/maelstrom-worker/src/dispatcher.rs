//! Central processing module for the worker. Receive messages from the broker, executors, and
//! artifact fetchers. Start or cancel jobs as appropriate via executors.

mod tracker;

use crate::{
    cache::{Cache, CacheEntryKind, CacheFs, CacheKey, GetArtifact},
    config::Slots,
};
use anyhow::{Error, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    ArtifactType, JobCompleted, JobError, JobId, JobOutcome, JobResult, JobSpec, Sha256Digest,
};
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    mem,
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
pub trait DispatcherDeps {
    type JobHandle: Clone;

    /// Start a new job. The dispatcher expects a [`Message::JobCompleted`] message when the job
    /// completes. The dispatcher can call [`cancel_job`] if it wants the job to stop immediately.
    fn start_job(&mut self, jid: JobId, spec: JobSpec, path: PathBuf) -> Self::JobHandle;

    /// Cancel a running job using the [`JobHandle`] obtained from [`start_job`]. This must be
    /// resilient in the case where the process has already completed. There must always be a
    /// [`Message::JobCompleted`] message delivered for every job, regardless of whether it was
    /// canceled or not.
    fn cancel_job(&mut self, handle: Self::JobHandle);

    /// Start a task to unmount the fuse mount.
    fn clean_up_fuse_handle_on_task(&mut self, handle: Self::JobHandle);

    /// A handle used to cancel a timer.
    type TimerHandle;

    /// Start a timer that will send a [`Message::JobTimer`] message when it's done, unless it is
    /// canceled beforehand.
    fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle;

    /// Cancel a timer previously started by [`start_timer`]. There's no guarantee on timing, so
    /// it's possible that a [`Message::JobTimer`] message may still be delivered for this job even
    /// after the timer is canceled.
    fn cancel_timer(&mut self, handle: Self::TimerHandle);

    /// Send a message to the broker.
    fn send_message_to_broker(&mut self, message: WorkerToBroker);

    /// Start a thread that will download an artifact from the broker and extract it into `path`.
    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf);

    /// Start a task that will build a layer-fs bottom layer out of an artifact
    fn build_bottom_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: PathBuf,
        artifact_type: ArtifactType,
        artifact_path: PathBuf,
    );

    /// Start a task that will build a layer-fs upper layer by stacking a bottom layer
    fn build_upper_fs_layer(
        &mut self,
        digest: Sha256Digest,
        layer_path: PathBuf,
        lower_layer_path: PathBuf,
        upper_layer_path: PathBuf,
    );

    /// Start a task to read the digests out of the given path to a manfiest.
    fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId);
}

/// The [`Cache`] dependency for [`Dispatcher`]. This should be exactly the same as [`Cache`]'s
/// public interface. We have this so we can isolate [`Dispatcher`] when testing.
pub trait DispatcherCache {
    fn get_artifact(
        &mut self,
        kind: CacheEntryKind,
        artifact: Sha256Digest,
        jid: JobId,
    ) -> GetArtifact;
    fn got_artifact_failure(&mut self, kind: CacheEntryKind, digest: &Sha256Digest) -> Vec<JobId>;
    fn got_artifact_success(
        &mut self,
        kind: CacheEntryKind,
        digest: &Sha256Digest,
        bytes_used: u64,
    ) -> (PathBuf, Vec<JobId>);
    fn decrement_ref_count(&mut self, kind: CacheEntryKind, digest: &Sha256Digest);
}

/// The standard implementation of [`DispatcherCache`] that just calls into [`Cache`].
impl<FsT: CacheFs> DispatcherCache for Cache<FsT> {
    fn get_artifact(
        &mut self,
        kind: CacheEntryKind,
        artifact: Sha256Digest,
        jid: JobId,
    ) -> GetArtifact {
        self.get_artifact(kind, artifact, jid)
    }

    fn got_artifact_failure(&mut self, kind: CacheEntryKind, digest: &Sha256Digest) -> Vec<JobId> {
        self.got_artifact_failure(kind, digest)
    }

    fn got_artifact_success(
        &mut self,
        kind: CacheEntryKind,
        digest: &Sha256Digest,
        bytes_used: u64,
    ) -> (PathBuf, Vec<JobId>) {
        self.got_artifact_success(kind, digest, bytes_used)
    }

    fn decrement_ref_count(&mut self, kind: CacheEntryKind, digest: &Sha256Digest) {
        self.decrement_ref_count(kind, digest)
    }
}

/// An input message for the dispatcher. These come from the broker, an executor, or an artifact
/// fetcher.
#[derive(Debug)]
pub enum Message {
    Broker(BrokerToWorker),
    JobCompleted(JobId, JobResult<JobCompleted, String>),
    JobTimer(JobId),
    ArtifactFetcher(Sha256Digest, Result<u64>),
    BuiltBottomFsLayer(Sha256Digest, Result<u64>),
    BuiltUpperFsLayer(Sha256Digest, Result<u64>),
    ReadManifestDigests(Sha256Digest, JobId, Result<HashSet<Sha256Digest>>),
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    /// Create a new dispatcher with the provided slot count. The slot count must be a positive
    /// number.
    pub fn new(deps: DepsT, cache: CacheT, slots: Slots) -> Self {
        Dispatcher {
            deps,
            cache,
            slots: slots.into_inner().into(),
            awaiting_layers: HashMap::default(),
            queued: VecDeque::default(),
            executing: HashMap::default(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [Message] for more information.
    pub fn receive_message(&mut self, msg: Message) {
        match msg {
            Message::Broker(BrokerToWorker::EnqueueJob(jid, spec)) => {
                self.receive_enqueue_job(jid, spec)
            }
            Message::Broker(BrokerToWorker::CancelJob(jid)) => self.receive_cancel_job(jid),
            Message::JobCompleted(jid, result) => self.receive_job_completed(jid, result),
            Message::JobTimer(jid) => self.receive_job_timer(jid),
            Message::ArtifactFetcher(digest, Err(err)) => {
                self.receive_artifact_failure(digest, err)
            }
            Message::ArtifactFetcher(digest, Ok(bytes_used)) => {
                self.receive_artifact_success(digest, bytes_used)
            }
            Message::BuiltBottomFsLayer(digest, Ok(bytes_used)) => {
                self.build_bottom_fs_layer_success(digest, bytes_used)
            }
            Message::BuiltBottomFsLayer(digest, Err(err)) => {
                self.build_bottom_fs_layer_failure(digest, err)
            }
            Message::BuiltUpperFsLayer(digest, Ok(bytes_used)) => {
                self.build_upper_fs_layer_success(digest, bytes_used)
            }
            Message::BuiltUpperFsLayer(digest, Err(err)) => {
                self.build_upper_fs_layer_failure(digest, err)
            }
            Message::ReadManifestDigests(digest, jid, Ok(digests)) => {
                self.read_manifest_digests_success(digest, jid, digests)
            }
            Message::ReadManifestDigests(digest, jid, Err(err)) => {
                self.read_manifest_digests_failure(digest, jid, err)
            }
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

struct AwaitingLayersJob {
    spec: JobSpec,
    tracker: LayerTracker,
}

struct AvailableJob {
    jid: JobId,
    spec: JobSpec,
    path: PathBuf,
    cache_keys: HashSet<CacheKey>,
}

enum ExecutingJobState<DepsT: DispatcherDeps> {
    Ok {
        handle: DepsT::JobHandle,
        timer: Option<DepsT::TimerHandle>,
    },
    Canceled,
    TimedOut,
}

struct ExecutingJob<DepsT: DispatcherDeps> {
    state: ExecutingJobState<DepsT>,
    cache_keys: HashSet<CacheKey>,
}

impl<DepsT: DispatcherDeps> ExecutingJob<DepsT> {
    fn new(
        handle: DepsT::JobHandle,
        cache_keys: HashSet<CacheKey>,
        timer: Option<DepsT::TimerHandle>,
    ) -> Self {
        ExecutingJob {
            state: ExecutingJobState::Ok { handle, timer },
            cache_keys,
        }
    }
}

struct Fetcher<'dispatcher, DepsT, CacheT> {
    deps: &'dispatcher mut DepsT,
    cache: &'dispatcher mut CacheT,
    jid: JobId,
}

impl<'dispatcher, DepsT, CacheT> tracker::Fetcher for Fetcher<'dispatcher, DepsT, CacheT>
where
    DepsT: DispatcherDeps,
    CacheT: DispatcherCache,
{
    fn fetch_artifact(&mut self, digest: &Sha256Digest) -> FetcherResult {
        match self
            .cache
            .get_artifact(CacheEntryKind::Blob, digest.clone(), self.jid)
        {
            GetArtifact::Success(path) => FetcherResult::Got(path),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get(path) => {
                self.deps.start_artifact_fetch(digest.clone(), path);
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
            .get_artifact(CacheEntryKind::BottomFsLayer, digest.clone(), self.jid)
        {
            GetArtifact::Success(path) => FetcherResult::Got(path),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get(path) => {
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
            .get_artifact(CacheEntryKind::UpperFsLayer, digest.clone(), self.jid)
        {
            GetArtifact::Success(path) => FetcherResult::Got(path),
            GetArtifact::Wait => FetcherResult::Pending,
            GetArtifact::Get(path) => {
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

/// Manage jobs based on the slot count and requests from the broker. If the broker sends more job
/// requests than there are slots, the extra requests are queued in a FIFO queue. It's up to the
/// broker to order the requests properly.
///
/// All methods are completely nonblocking. They will never block the task or the thread.
pub struct Dispatcher<DepsT: DispatcherDeps, CacheT> {
    deps: DepsT,
    cache: CacheT,
    slots: usize,
    awaiting_layers: HashMap<JobId, AwaitingLayersJob>,
    queued: VecDeque<AvailableJob>,
    executing: HashMap<JobId, ExecutingJob<DepsT>>,
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    fn possibly_start_job(&mut self) {
        while self.executing.len() < self.slots && !self.queued.is_empty() {
            let AvailableJob {
                jid,
                spec,
                path,
                cache_keys,
            } = self.queued.pop_front().unwrap();
            let timeout = spec.timeout;
            let executing_job = ExecutingJob::new(
                self.deps.start_job(jid, spec, path),
                cache_keys,
                timeout.map(|timeout| self.deps.start_timer(jid, Duration::from(timeout))),
            );
            self.executing.insert(jid, executing_job).assert_is_none();
        }
    }

    fn enqueue_job_with_all_layers(&mut self, jid: JobId, spec: JobSpec, tracker: LayerTracker) {
        let (path, cache_keys) = tracker.into_path_and_cache_keys();
        self.queued.push_back(AvailableJob {
            jid,
            spec,
            path,
            cache_keys,
        });
        self.possibly_start_job();
    }

    fn receive_enqueue_job(&mut self, jid: JobId, spec: JobSpec) {
        let mut fetcher = Fetcher {
            deps: &mut self.deps,
            cache: &mut self.cache,
            jid,
        };
        let tracker = LayerTracker::new(&spec.layers, &mut fetcher);
        if tracker.is_complete() {
            self.enqueue_job_with_all_layers(jid, spec, tracker);
        } else {
            self.awaiting_layers
                .insert(jid, AwaitingLayersJob { spec, tracker })
                .assert_is_none();
        }
    }

    fn receive_cancel_job(&mut self, jid: JobId) {
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // We may have already gotten some layers. Make sure we release those.
            for CacheKey { kind, digest } in entry.tracker.into_cache_keys() {
                self.cache.decrement_ref_count(kind, &digest);
            }
        } else if let Some(&mut ExecutingJob { ref mut state, .. }) = self.executing.get_mut(&jid) {
            // The job was executing. We kill the job and cancel a timer if there is one, but we
            // wait around until it's actually teriminated. We don't want to release the layers
            // until then. If we didn't it would be possible for us to try to remove a directory
            // that was still in use, which would fail.
            if let ExecutingJobState::Ok { handle, timer } =
                mem::replace(state, ExecutingJobState::Canceled)
            {
                self.deps.cancel_job(handle.clone());
                if let Some(handle) = timer {
                    self.deps.cancel_timer(handle)
                }
                self.deps.clean_up_fuse_handle_on_task(handle);
            }
        } else {
            // It may be the queue.
            self.queued.retain_mut(|entry| {
                if entry.jid != jid {
                    true
                } else {
                    for CacheKey { kind, digest } in &entry.cache_keys {
                        self.cache.decrement_ref_count(*kind, digest);
                    }
                    false
                }
            });
        }
    }

    fn receive_job_completed(&mut self, jid: JobId, result: JobResult<JobCompleted, String>) {
        let Some(job) = self.executing.remove(&jid) else {
            panic!("missing entry for {jid:?}");
        };

        match job.state {
            ExecutingJobState::Ok { handle, timer } => {
                if let Some(handle) = timer {
                    self.deps.cancel_timer(handle)
                }
                self.deps.clean_up_fuse_handle_on_task(handle);
                self.deps
                    .send_message_to_broker(WorkerToBroker(jid, result.map(JobOutcome::Completed)));
            }
            ExecutingJobState::Canceled => {}
            ExecutingJobState::TimedOut => self.deps.send_message_to_broker(WorkerToBroker(
                jid,
                result.map(|c| JobOutcome::TimedOut(c.effects)),
            )),
        }
        for CacheKey { kind, digest } in job.cache_keys {
            self.cache.decrement_ref_count(kind, &digest);
        }
        self.possibly_start_job();
    }

    fn receive_job_timer(&mut self, jid: JobId) {
        if let Some(&mut ExecutingJob {
            ref mut state,
            cache_keys: _,
        }) = self.executing.get_mut(&jid)
        {
            // The job was executing. We kill the job, but we wait around until it's actually
            // teriminated. We don't want to release the layers until the job has terminated. If we
            // didn't it would be possible for us to try to remove a directory that was still in
            // use, which would fail.
            match state {
                ExecutingJobState::Ok { handle, timer: _ } => {
                    self.deps.cancel_job(handle.clone());
                    *state = ExecutingJobState::TimedOut;
                }
                ExecutingJobState::TimedOut => {
                    panic!("two timer expirations for job {jid:?}");
                }
                ExecutingJobState::Canceled => {}
            }
        }
    }

    fn job_failure(&mut self, digest: &Sha256Digest, jid: JobId, msg: &str, err: &Error) {
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // If this was the first layer error for this request, then we'll find something in
            // the hash table, and we'll need to clean up.
            //
            // Otherwise, it means that there were previous errors for this entry, or it was
            // canceled, and there's nothing to do here.
            self.deps.send_message_to_broker(WorkerToBroker(
                jid,
                Err(JobError::System(format!("{msg} {digest}: {err}"))),
            ));
            for CacheKey { kind, digest } in entry.tracker.into_cache_keys() {
                self.cache.decrement_ref_count(kind, &digest);
            }
        }
    }

    fn cache_fill_failure(
        &mut self,
        kind: CacheEntryKind,
        digest: Sha256Digest,
        msg: &str,
        err: Error,
    ) {
        for jid in self.cache.got_artifact_failure(kind, &digest) {
            self.job_failure(&digest, jid, msg, &err)
        }
    }

    fn receive_artifact_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to download and extract layer artifact";
        self.cache_fill_failure(CacheEntryKind::Blob, digest, msg, err)
    }

    fn advance_job(
        &mut self,
        jid: JobId,
        kind: CacheEntryKind,
        digest: &Sha256Digest,
        cb: impl FnOnce(&mut LayerTracker, &Sha256Digest, &mut Fetcher<'_, DepsT, CacheT>),
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
                    cache: &mut self.cache,
                    jid,
                };
                cb(&mut entry.get_mut().tracker, digest, &mut fetcher);
                if entry.get().tracker.is_complete() {
                    let AwaitingLayersJob { spec, tracker } = entry.remove();
                    self.enqueue_job_with_all_layers(jid, spec, tracker);
                }
            }
        }
    }

    fn cache_fill_success(
        &mut self,
        kind: CacheEntryKind,
        digest: Sha256Digest,
        bytes_used: u64,
        cb: impl Fn(&mut LayerTracker, &Sha256Digest, PathBuf, &mut Fetcher<'_, DepsT, CacheT>),
    ) {
        let (path, jobs) = self.cache.got_artifact_success(kind, &digest, bytes_used);
        for jid in jobs {
            self.advance_job(jid, kind, &digest, |tracker, digest, fetcher| {
                cb(tracker, digest, path.clone(), fetcher)
            });
        }
    }

    fn receive_artifact_success(&mut self, digest: Sha256Digest, bytes_used: u64) {
        self.cache_fill_success(
            CacheEntryKind::Blob,
            digest,
            bytes_used,
            |tracker, digest, path, fetcher| tracker.got_artifact(digest, path, fetcher),
        )
    }

    fn build_bottom_fs_layer_success(&mut self, digest: Sha256Digest, bytes_used: u64) {
        self.cache_fill_success(
            CacheEntryKind::BottomFsLayer,
            digest,
            bytes_used,
            |tracker, digest, path, fetcher| tracker.got_bottom_fs_layer(digest, path, fetcher),
        )
    }

    fn build_bottom_fs_layer_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to build bottom FS layer";
        self.cache_fill_failure(CacheEntryKind::BottomFsLayer, digest, msg, err)
    }

    fn build_upper_fs_layer_success(&mut self, digest: Sha256Digest, bytes_used: u64) {
        self.cache_fill_success(
            CacheEntryKind::UpperFsLayer,
            digest,
            bytes_used,
            |tracker, digest, path, fetcher| tracker.got_upper_fs_layer(digest, path, fetcher),
        )
    }

    fn build_upper_fs_layer_failure(&mut self, digest: Sha256Digest, err: Error) {
        let msg = "Failed to build upper FS layer";
        self.cache_fill_failure(CacheEntryKind::UpperFsLayer, digest, msg, err)
    }

    fn read_manifest_digests_success(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        digests: HashSet<Sha256Digest>,
    ) {
        self.advance_job(
            jid,
            CacheEntryKind::Blob,
            &digest,
            move |tracker, digest, fetcher| {
                tracker.got_manifest_digests(digest, digests, fetcher);
            },
        );
    }

    fn read_manifest_digests_failure(&mut self, digest: Sha256Digest, jid: JobId, err: Error) {
        self.job_failure(&digest, jid, "failed to read manifest", &err);
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
    use crate::cache::CacheEntryKind::*;
    use anyhow::anyhow;
    use maelstrom_base::{self as base, JobEffects, JobOutputResult, JobStatus};
    use maelstrom_test::*;
    use std::{cell::RefCell, rc::Rc, time::Duration};
    use BrokerToWorker::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum TestMessage {
        StartJob(JobId, JobSpec, PathBuf),
        SendMessageToBroker(WorkerToBroker),
        StartArtifactFetch(Sha256Digest, PathBuf),
        BuildBottomFsLayer(Sha256Digest, PathBuf, ArtifactType, PathBuf),
        BuildUpperFsLayer(Sha256Digest, PathBuf, PathBuf, PathBuf),
        ReadManifestDigests(Sha256Digest, PathBuf, JobId),
        CacheGetArtifact(CacheEntryKind, Sha256Digest, JobId),
        CacheGotArtifactSuccess(CacheEntryKind, Sha256Digest, u64),
        CacheGotArtifactFailure(CacheEntryKind, Sha256Digest),
        CacheDecrementRefCount(CacheEntryKind, Sha256Digest),
        Kill(JobId),
        StartTimer(JobId, Duration),
        CancelTimer(JobId),
    }

    use TestMessage::*;

    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<CacheKey, GetArtifact>,
        got_artifact_success_returns: HashMap<CacheKey, (PathBuf, Vec<JobId>)>,
        got_artifact_failure_returns: HashMap<CacheKey, Vec<JobId>>,
    }

    impl DispatcherDeps for Rc<RefCell<TestState>> {
        type JobHandle = JobId;

        fn start_job(&mut self, jid: JobId, spec: JobSpec, path: PathBuf) -> Self::JobHandle {
            let mut mut_ref = self.borrow_mut();
            mut_ref.messages.push(StartJob(jid, spec, path));
            jid
        }

        fn clean_up_fuse_handle_on_task(&mut self, _handle: Self::JobHandle) {}

        fn cancel_job(&mut self, handle: Self::JobHandle) {
            self.borrow_mut().messages.push(Kill(handle));
        }

        type TimerHandle = JobId;

        fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
            self.borrow_mut().messages.push(StartTimer(jid, duration));
            jid
        }

        fn cancel_timer(&mut self, handle: Self::TimerHandle) {
            self.borrow_mut().messages.push(CancelTimer(handle));
        }

        fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
            self.borrow_mut()
                .messages
                .push(StartArtifactFetch(digest, path));
        }

        fn build_bottom_fs_layer(
            &mut self,
            digest: Sha256Digest,
            layer_path: PathBuf,
            artifact_type: ArtifactType,
            artifact_path: PathBuf,
        ) {
            self.borrow_mut().messages.push(BuildBottomFsLayer(
                digest,
                layer_path,
                artifact_type,
                artifact_path,
            ));
        }

        fn build_upper_fs_layer(
            &mut self,
            digest: Sha256Digest,
            layer_path: PathBuf,
            lower_layer_path: PathBuf,
            upper_layer_path: PathBuf,
        ) {
            self.borrow_mut().messages.push(BuildUpperFsLayer(
                digest,
                layer_path,
                lower_layer_path,
                upper_layer_path,
            ));
        }

        fn read_manifest_digests(&mut self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ReadManifestDigests(digest, path, jid));
        }

        fn send_message_to_broker(&mut self, message: WorkerToBroker) {
            self.borrow_mut()
                .messages
                .push(SendMessageToBroker(message));
        }
    }

    impl DispatcherCache for Rc<RefCell<TestState>> {
        fn get_artifact(
            &mut self,
            kind: CacheEntryKind,
            digest: Sha256Digest,
            jid: JobId,
        ) -> GetArtifact {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifact(kind, digest.clone(), jid));
            self.borrow_mut()
                .get_artifact_returns
                .remove(&CacheKey::new(kind, digest.clone()))
                .expect(&format!("unexpected get_artifact of {kind:?} {digest}"))
        }

        fn got_artifact_failure(
            &mut self,
            kind: CacheEntryKind,
            digest: &Sha256Digest,
        ) -> Vec<JobId> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactFailure(kind, digest.clone()));
            self.borrow_mut()
                .got_artifact_failure_returns
                .remove(&CacheKey::new(kind, digest.clone()))
                .unwrap()
        }

        fn got_artifact_success(
            &mut self,
            kind: CacheEntryKind,
            digest: &Sha256Digest,
            bytes_used: u64,
        ) -> (PathBuf, Vec<JobId>) {
            self.borrow_mut().messages.push(CacheGotArtifactSuccess(
                kind,
                digest.clone(),
                bytes_used,
            ));
            self.borrow_mut()
                .got_artifact_success_returns
                .remove(&CacheKey::new(kind, digest.clone()))
                .unwrap()
        }

        fn decrement_ref_count(&mut self, kind: CacheEntryKind, digest: &Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefCount(kind, digest.clone()))
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        dispatcher: Dispatcher<Rc<RefCell<TestState>>, Rc<RefCell<TestState>>>,
    }

    impl Fixture {
        fn new<const L: usize, const M: usize, const N: usize>(
            slots: u16,
            get_artifact_returns: [(CacheKey, GetArtifact); L],
            got_artifact_success_returns: [(CacheKey, (PathBuf, Vec<JobId>)); M],
            got_artifact_failure_returns: [(CacheKey, Vec<JobId>); N],
        ) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
                get_artifact_returns: HashMap::from(get_artifact_returns),
                got_artifact_success_returns: HashMap::from(got_artifact_success_returns),
                got_artifact_failure_returns: HashMap::from(got_artifact_failure_returns),
            }));
            let dispatcher = Dispatcher::new(
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
            CacheKey::new(UpperFsLayer, upper_digest!($($d),*))
        };
        ($kind:expr, $d:expr) => {
            CacheKey::new($kind, digest!($d))
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
        enqueue_no_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/a")),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
        };
    }

    script_test! {
        enqueue_immediate_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/a"))
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
        };
    }

    script_test! {
        enqueue_mixed_artifacts_no_error_slots_available,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Get(path_buf!("/b"))),
            (cache_key!(Blob, 43), GetArtifact::Wait),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (43, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(Blob, digest!(43), jid!(1)),
            StartArtifactFetch(digest!(42), path_buf!("/b")),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
        };
    }

    /*
    script_test! {
        enqueue_immediate_artifacts_system_error_slots_available,
        Fixture::new(1, [
            Err(JobError::System(string!("se"))),
        ], [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(UpperFsLayer, 41, 42, 41), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (41, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(41, 42, 41), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (41, Tar)]), path_buf!("/a")),
            SendMessageToBroker(WorkerToBroker(jid!(1), Err(JobError::System(string!("se"))))),
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(41, 42, 41)),
        };
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        enqueue_immediate_artifacts_execution_error_slots_available,
        Fixture::new(1, [
            Err(JobError::Execution(string!("ee"))),
        ], [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/a")),
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Err(JobError::Execution(string!("ee"))))),
        };
        Broker(CancelJob(jid!(1))) => {};
    }
    */

    script_test! {
        enqueue_fills_all_slots,
        Fixture::new(4, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(Blob, 3), GetArtifact::Success(path_buf!("/c"))),
            (cache_key!(Blob, 4), GetArtifact::Success(path_buf!("/d"))),
            (cache_key!(Blob, 5), GetArtifact::Success(path_buf!("/e"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 3), GetArtifact::Success(path_buf!("/c"))),
            (cache_key!(BottomFsLayer, 4), GetArtifact::Success(path_buf!("/d"))),
            (cache_key!(BottomFsLayer, 5), GetArtifact::Success(path_buf!("/e"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/a")),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/b")),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar))) => {
            CacheGetArtifact(Blob, digest!(3), jid!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(3), jid!(3)),
            StartJob(jid!(3), spec!(3, Tar), path_buf!("/c")),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(Blob, digest!(4), jid!(4)),
            CacheGetArtifact(BottomFsLayer, digest!(4), jid!(4)),
            StartJob(jid!(4), spec!(4, Tar), path_buf!("/d")),
        };
        Broker(EnqueueJob(jid!(5), spec!(5, Tar))) => {
            CacheGetArtifact(Blob, digest!(5), jid!(5)),
            CacheGetArtifact(BottomFsLayer, digest!(5), jid!(5)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(5), spec!(5, Tar), path_buf!("/e")),
        }
    }

    /*
    script_test! {
        possibly_start_job_loops_until_slots_full,
        Fixture::new(1, [
            Ok(pid!(1)),
            Err(JobError::Execution(string!("ee"))),
            Err(JobError::System(string!("se"))),
            Ok(pid!(4)),
        ], [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(Blob, 3), GetArtifact::Success(path_buf!("/c"))),
            (cache_key!(Blob, 4), GetArtifact::Success(path_buf!("/d"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 3), GetArtifact::Success(path_buf!("/c"))),
            (cache_key!(BottomFsLayer, 4), GetArtifact::Success(path_buf!("/d"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/a")),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar))) => {
            CacheGetArtifact(Blob, digest!(3), jid!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(3), jid!(3)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(Blob, digest!(4), jid!(4)),
            CacheGetArtifact(BottomFsLayer, digest!(4), jid!(4)),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), outcome!(1))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/b")),
            SendMessageToBroker(WorkerToBroker(jid!(2), Err(JobError::Execution(string!("ee"))))),
            CacheDecrementRefCount(Blob, digest!(2)),
            CacheDecrementRefCount(BottomFsLayer, digest!(2)),
            StartJob(jid!(3), spec!(3, Tar), path_buf!("/c")),
            SendMessageToBroker(WorkerToBroker(jid!(3), Err(JobError::System(string!("se"))))),
            CacheDecrementRefCount(Blob, digest!(3)),
            CacheDecrementRefCount(BottomFsLayer, digest!(3)),
            StartJob(jid!(4), spec!(4, Tar), path_buf!("/d")),
        };
    }
    */

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Wait),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(41)),
        };
    }

    script_test! {
        cancel_executing,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(Blob, 43), GetArtifact::Success(path_buf!("/c"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 43), GetArtifact::Success(path_buf!("/c"))),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/a"))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, [(43, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(43), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(43), jid!(2)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            StartJob(jid!(2), spec!(2, [(43, Tar)]), path_buf!("/c")),
        };
    }

    script_test! {
        cancel_queued,
        Fixture::new(2, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/2"))),
            (cache_key!(Blob, 4), GetArtifact::Success(path_buf!("/4"))),
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/2"))),
            (cache_key!(BottomFsLayer, 4), GetArtifact::Success(path_buf!("/4"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(UpperFsLayer, 41, 42, 41), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/1")),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/2")),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, [(41, Tar), (42, Tar), (41, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(3)),
            CacheGetArtifact(Blob, digest!(42), jid!(3)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(3)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(3)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(41, 42, 41), jid!(3)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(Blob, digest!(4), jid!(4)),
            CacheGetArtifact(BottomFsLayer, digest!(4), jid!(4)),
        };
        Broker(CancelJob(jid!(3))) => {
            CacheDecrementRefCount(Blob, digest!(41)),
            CacheDecrementRefCount(BottomFsLayer, digest!(41)),
            CacheDecrementRefCount(Blob, digest!(42)),
            CacheDecrementRefCount(BottomFsLayer, digest!(42)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(42, 41)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(41, 42, 41)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), outcome!(1))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(4), spec!(4, Tar), path_buf!("/4")),
        };
    }

    script_test! {
        cancel_unknown,
        Fixture::new(1, [], [], []),
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_canceled,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/a")),
        };
        Broker(CancelJob(jid!(1))) => { Kill(jid!(1)) };
        Broker(CancelJob(jid!(1))) => {};
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_timed_out,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/2"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/1")),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        JobTimer(jid!(1)) => {
            Kill(jid!(1))
        };
        Broker(CancelJob(jid!(1))) => {};
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/2")),
        };
    }

    script_test! {
        receive_ok_job_completed_executing,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/a")),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), outcome!(1))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/b")),
        };
    }

    script_test! {
        receive_error_job_completed_executing,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf!("/a")),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        Message::JobCompleted(jid!(1), Err(JobError::System(string!("job error")))) => {
            SendMessageToBroker(WorkerToBroker(
                jid!(1), Err(JobError::System(string!("job error"))))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/b")),
        };
    }

    script_test! {
        receive_job_completed_canceled,
        Fixture::new(1, [
            (cache_key!(Blob, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(Blob, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(BottomFsLayer, 41), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 42), GetArtifact::Success(path_buf!("/b"))),
            (cache_key!(UpperFsLayer, 42, 41), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(42), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(42, 41), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf!("/a")),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Signaled(9),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
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
        let mut fixture = Fixture::new(1, [], [], []);
        fixture.dispatcher.receive_message(Message::JobCompleted(
            jid!(1),
            Ok(base::JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                },
            }),
        ));
    }

    script_test! {
        timer_scheduled_then_canceled_on_success,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)), path_buf!("/a")),
            StartTimer(jid!(1), Duration::from_secs(33)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed(base::JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                }
            })))),
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            CancelTimer(jid!(1)),
        };
    }

    script_test! {
        timer_scheduled_then_canceled_on_cancellation,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)), path_buf!("/a")),
            StartTimer(jid!(1), Duration::from_secs(33)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
            CancelTimer(jid!(1)),
        };
    }

    script_test! {
        time_out_running_1,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/2"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/1")),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        JobTimer(jid!(1)) => {
            Kill(jid!(1)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Inline(boxed_u8!(b"stderr")),
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::TimedOut(JobEffects {
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Inline(boxed_u8!(b"stderr")),
            })))),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/2")),
        };
    }

    script_test! {
        time_out_completed,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/2"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/1")),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            CancelTimer(jid!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed(base::JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                }
            })))),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/2")),
        };
        JobTimer(jid!(1)) => {};
    }

    script_test! {
        time_out_canceled,
        Fixture::new(1, [
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/2"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(BottomFsLayer, 2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf!("/1")),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(Blob, digest!(2), jid!(2)),
            CacheGetArtifact(BottomFsLayer, digest!(2), jid!(2)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(jid!(1)),
            CancelTimer(jid!(1)),
        };
        JobTimer(jid!(1)) => {};
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf!("/2")),
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
            (cache_key!(Blob, 41), (path_buf!("/a"), vec![jid!(1)])),
            (cache_key!(Blob, 43), (path_buf!("/c"), vec![jid!(1)])),
        ], [
            (cache_key!(Blob, 42), vec![jid!(1)]),
            (cache_key!(Blob, 44), vec![jid!(1)]),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (43, Tar), (44, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(41), jid!(1)),
            CacheGetArtifact(Blob, digest!(42), jid!(1)),
            CacheGetArtifact(Blob, digest!(43), jid!(1)),
            CacheGetArtifact(Blob, digest!(44), jid!(1)),
        };
        ArtifactFetcher(digest!(41), Ok(101)) => {
            CacheGotArtifactSuccess(Blob, digest!(41), 101),
            CacheGetArtifact(BottomFsLayer, digest!(41), jid!(1)),
        };
        ArtifactFetcher(digest!(42), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(Blob, digest!(42)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Err(JobError::System(
                string!("Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a: foo"))))),
            CacheDecrementRefCount(Blob, digest!(41))
        };
        ArtifactFetcher(digest!(43), Ok(103)) => {
            CacheGotArtifactSuccess(Blob, digest!(43), 103),
            CacheDecrementRefCount(Blob, digest!(43))
        };
        ArtifactFetcher(digest!(44), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(Blob, digest!(44)),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.is_none()")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(
            2,
            [
                (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/a"))),
                (cache_key!(Blob, 2), GetArtifact::Success(path_buf!("/b"))),
                (
                    cache_key!(BottomFsLayer, 1),
                    GetArtifact::Success(path_buf!("/a")),
                ),
                (
                    cache_key!(BottomFsLayer, 2),
                    GetArtifact::Success(path_buf!("/b")),
                ),
            ],
            [],
            [],
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
            (cache_key!(Blob, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(BottomFsLayer, 1), GetArtifact::Success(path_buf!("/1"))),
            (cache_key!(UpperFsLayer, 1, 1), GetArtifact::Success(path_buf!("/1"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(1, Tar), (1, Tar)]))) => {
            CacheGetArtifact(Blob, digest!(1), jid!(1)),
            CacheGetArtifact(BottomFsLayer, digest!(1), jid!(1)),
            CacheGetArtifact(UpperFsLayer, upper_digest!(1, 1), jid!(1)),
            StartJob(jid!(1), spec!(1, [(1, Tar), (1, Tar)]), path_buf!("/1")),
        };
        Message::JobCompleted(jid!(1), Ok(base::JobCompleted {
            status: JobStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        })) => {
            CacheDecrementRefCount(Blob, digest!(1)),
            CacheDecrementRefCount(BottomFsLayer, digest!(1)),
            CacheDecrementRefCount(UpperFsLayer, upper_digest!(1, 1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed(base::JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                },
            })))),
        };
    }
}
