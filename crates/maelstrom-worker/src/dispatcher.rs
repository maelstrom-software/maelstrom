//! Central processing module for the worker. Receive messages from the broker, executors, and
//! artifact fetchers. Start or cancel jobs as appropriate via executors.

mod tracker;

use crate::{
    cache::{Cache, CacheFs, GetArtifact},
    config::Slots,
};
use anyhow::{Error, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    ArtifactType, JobEffects, JobError, JobId, JobOutcome, JobOutputResult, JobResult, JobSpec,
    JobStatus, NonEmpty, Sha256Digest,
};
use maelstrom_linux::Pid;
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    mem,
    path::PathBuf,
    result::Result as StdResult,
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
    /// Start a new job. If this returns [`JobResult::Ok`], then the dispatcher is expecting three
    /// separate messages to know when the job has completed: [`Message::PidStatus`],
    /// [`Message::JobStdout`], and [`Message::JobStderr`]. These messages don't have to come in
    /// any particular order, but the dispatcher won't proceed with the next job until all three
    /// have arrived.
    fn start_job(
        &mut self,
        jid: JobId,
        spec: JobSpec,
        layers: NonEmpty<PathBuf>,
    ) -> (JobResult<Pid, String>, NonEmpty<Sha256Digest>);

    /// Kill a running job using the [`Pid`] obtained from [`JobResult::Ok`]. This must be
    /// resilient in the case where the process has already completed.
    fn kill_job(&mut self, pid: Pid);

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
    fn start_artifact_fetch(&mut self, digest: Sha256Digest, type_: ArtifactType, path: PathBuf);
}

/// The [`Cache`] dependency for [`Dispatcher`]. This should be exactly the same as [`Cache`]'s
/// public interface. We have this so we can isolate [`Dispatcher`] when testing.
pub trait DispatcherCache {
    fn get_artifact(&mut self, artifact: Sha256Digest, jid: JobId) -> GetArtifact;
    fn got_artifact_failure(&mut self, digest: &Sha256Digest) -> Vec<JobId>;
    fn got_artifact_success(
        &mut self,
        digest: &Sha256Digest,
        bytes_used: u64,
    ) -> (PathBuf, Vec<JobId>);
    fn decrement_ref_count(&mut self, digest: &Sha256Digest);
}

/// The standard implementation of [`DispatcherCache`] that just calls into [`Cache`].
impl<FsT: CacheFs> DispatcherCache for Cache<FsT> {
    fn get_artifact(&mut self, artifact: Sha256Digest, jid: JobId) -> GetArtifact {
        self.get_artifact(artifact, jid)
    }

    fn got_artifact_failure(&mut self, digest: &Sha256Digest) -> Vec<JobId> {
        self.got_artifact_failure(digest)
    }

    fn got_artifact_success(
        &mut self,
        digest: &Sha256Digest,
        bytes_used: u64,
    ) -> (PathBuf, Vec<JobId>) {
        self.got_artifact_success(digest, bytes_used)
    }

    fn decrement_ref_count(&mut self, digest: &Sha256Digest) {
        self.decrement_ref_count(digest)
    }
}

/// An input message for the dispatcher. These come from the broker, an executor, or an artifact
/// fetcher.
#[derive(Debug)]
pub enum Message {
    Broker(BrokerToWorker),
    PidStatus(Pid, JobStatus),
    JobStdout(JobId, StdResult<JobOutputResult, String>),
    JobStderr(JobId, StdResult<JobOutputResult, String>),
    JobTimer(JobId),
    ArtifactFetcher(Sha256Digest, Result<u64>),
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
            executing_pids: HashMap::default(),
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
            Message::PidStatus(pid, status) => self.receive_pid_status(pid, status),
            Message::JobStdout(jid, result) => self.receive_job_stdout(jid, result),
            Message::JobStderr(jid, result) => self.receive_job_stderr(jid, result),
            Message::JobTimer(jid) => self.receive_job_timer(jid),
            Message::ArtifactFetcher(digest, Err(err)) => {
                self.receive_artifact_failure(digest, err)
            }
            Message::ArtifactFetcher(digest, Ok(bytes_used)) => {
                self.receive_artifact_success(digest, bytes_used)
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

struct AwaitingLayersEntry {
    spec: JobSpec,
    tracker: LayerTracker,
}

enum ExecutingJobState<DepsT: DispatcherDeps> {
    Ok {
        pid: Pid,
        timer: Option<DepsT::TimerHandle>,
    },
    Canceled,
    TimedOut,
}

struct ExecutingJob<DepsT: DispatcherDeps> {
    state: ExecutingJobState<DepsT>,
    status: Option<JobStatus>,
    stdout: Option<StdResult<JobOutputResult, String>>,
    stderr: Option<StdResult<JobOutputResult, String>>,
    digests: HashSet<Sha256Digest>,
}

impl<DepsT: DispatcherDeps> ExecutingJob<DepsT> {
    fn new(pid: Pid, digests: HashSet<Sha256Digest>, timer: Option<DepsT::TimerHandle>) -> Self {
        ExecutingJob {
            state: ExecutingJobState::Ok { pid, timer },
            status: None,
            stdout: None,
            stderr: None,
            digests,
        }
    }

    fn is_complete(&self) -> bool {
        self.status.is_some() && self.stdout.is_some() && self.stderr.is_some()
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
    awaiting_layers: HashMap<JobId, AwaitingLayersEntry>,
    queued: VecDeque<(JobId, JobSpec, LayerTracker)>,
    executing: HashMap<JobId, ExecutingJob<DepsT>>,
    executing_pids: HashMap<Pid, JobId>,
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    fn possibly_start_job(&mut self) {
        while self.executing.len() < self.slots && !self.queued.is_empty() {
            let (jid, spec, tracker) = self.queued.pop_front().unwrap();
            let timeout = spec.timeout;
            let (paths, digests) = tracker.into_paths_and_digests();
            match self.deps.start_job(jid, spec, paths) {
                (Ok(pid), _) => {
                    let executing_job = ExecutingJob::new(
                        pid,
                        digests,
                        timeout.map(|timeout| self.deps.start_timer(jid, Duration::from(timeout))),
                    );
                    self.executing.insert(jid, executing_job).assert_is_none();
                    self.executing_pids.insert(pid, jid).assert_is_none();
                }
                (Err(e), _) => {
                    for digest in digests {
                        self.cache.decrement_ref_count(&digest);
                    }
                    self.deps
                        .send_message_to_broker(WorkerToBroker(jid, Err(e)));
                }
            }
        }
    }

    fn enqueue_job_with_all_layers(&mut self, jid: JobId, spec: JobSpec, tracker: LayerTracker) {
        self.queued.push_back((jid, spec, tracker));
        self.possibly_start_job();
    }

    fn receive_enqueue_job(&mut self, jid: JobId, spec: JobSpec) {
        let tracker = LayerTracker::new(&spec.layers, |digest, type_| -> FetcherResult {
            match self.cache.get_artifact(digest.clone(), jid) {
                GetArtifact::Success(path) => FetcherResult::Got(path),
                GetArtifact::Wait => FetcherResult::Pending,
                GetArtifact::Get(path) => {
                    self.deps.start_artifact_fetch(digest.clone(), type_, path);
                    FetcherResult::Pending
                }
            }
        });
        if tracker.is_complete() {
            self.enqueue_job_with_all_layers(jid, spec, tracker);
        } else {
            self.awaiting_layers
                .insert(jid, AwaitingLayersEntry { spec, tracker })
                .assert_is_none();
        }
    }

    fn receive_cancel_job(&mut self, jid: JobId) {
        let mut digests: Option<HashSet<Sha256Digest>> = None;
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // We may have already gotten some layers. Make sure we release those.
            digests = Some(entry.tracker.into_digests());
        } else if let Some(&mut ExecutingJob { ref mut state, .. }) = self.executing.get_mut(&jid) {
            // The job was executing. We kill the job and cancel a timer if there is one, but we
            // wait around until it's actually teriminated. We don't want to release the layers
            // until then. If we didn't it would be possible for us to try to remove a directory
            // that was still in use, which would fail.
            if let ExecutingJobState::Ok { pid, timer } =
                mem::replace(state, ExecutingJobState::Canceled)
            {
                self.deps.kill_job(pid);
                if let Some(handle) = timer {
                    self.deps.cancel_timer(handle)
                }
            }
        } else {
            // It may be the queue.
            self.queued = mem::take(&mut self.queued)
                .into_iter()
                .filter_map(|x| {
                    if x.0 != jid {
                        Some(x)
                    } else {
                        assert!(digests.is_none());
                        digests = Some(x.1.layers.into_iter().map(|(digest, _)| digest).collect());
                        None
                    }
                })
                .collect();
        }
        if let Some(digests) = digests {
            for digest in digests {
                self.cache.decrement_ref_count(&digest);
            }
        }
    }

    fn update_entry_and_potentially_finish_job(
        &mut self,
        jid: JobId,
        f: impl FnOnce(&mut ExecutingJob<DepsT>),
    ) {
        let Entry::Occupied(mut oe) = self.executing.entry(jid) else {
            panic!("missing entry for {jid:?}");
        };
        f(oe.get_mut());
        if oe.get().is_complete() {
            let (
                jid,
                ExecutingJob {
                    state,
                    status,
                    stdout,
                    stderr,
                    digests,
                },
            ) = oe.remove_entry();
            let effects_result = match (stdout.unwrap(), stderr.unwrap()) {
                (StdResult::Ok(stdout), StdResult::Ok(stderr)) => Ok(JobEffects { stdout, stderr }),
                (StdResult::Err(e), _) | (_, StdResult::Err(e)) => Err(JobError::System(e)),
            };

            match state {
                ExecutingJobState::Ok { pid: _, timer } => {
                    if let Some(handle) = timer {
                        self.deps.cancel_timer(handle)
                    }
                    let status = status.unwrap();
                    self.deps.send_message_to_broker(WorkerToBroker(
                        jid,
                        effects_result.map(|effects| JobOutcome::Completed { status, effects }),
                    ));
                }
                ExecutingJobState::Canceled => {}
                ExecutingJobState::TimedOut => {
                    self.deps.send_message_to_broker(WorkerToBroker(
                        jid,
                        effects_result.map(JobOutcome::TimedOut),
                    ));
                }
            }
            for digest in digests {
                self.cache.decrement_ref_count(&digest);
            }
            self.possibly_start_job();
        }
    }

    fn receive_pid_status(&mut self, pid: Pid, status: JobStatus) {
        if let Some(jid) = self.executing_pids.remove(&pid) {
            self.update_entry_and_potentially_finish_job(jid, move |entry| {
                entry.status = Some(status)
            });
        }
    }

    fn receive_job_stdout(&mut self, jid: JobId, result: StdResult<JobOutputResult, String>) {
        self.update_entry_and_potentially_finish_job(jid, move |entry| entry.stdout = Some(result));
    }

    fn receive_job_stderr(&mut self, jid: JobId, result: StdResult<JobOutputResult, String>) {
        self.update_entry_and_potentially_finish_job(jid, move |entry| entry.stderr = Some(result));
    }

    fn receive_job_timer(&mut self, jid: JobId) {
        if let Some(&mut ExecutingJob {
            ref mut state,
            ref status,
            ..
        }) = self.executing.get_mut(&jid)
        {
            // The job was executing. We kill the job, but we wait around until it's actually
            // teriminated. We don't want to release the layers until the job has terminated. If we
            // didn't it would be possible for us to try to remove a directory that was still in
            // use, which would fail.
            match state {
                ExecutingJobState::Ok { pid, timer: _ } => {
                    if status.is_none() {
                        self.deps.kill_job(*pid);
                    }
                    *state = ExecutingJobState::TimedOut;
                }
                ExecutingJobState::TimedOut => {
                    panic!("two timer expirations for job {jid:?}");
                }
                ExecutingJobState::Canceled => {}
            }
        }
    }

    fn receive_artifact_failure(&mut self, digest: Sha256Digest, err: Error) {
        for jid in self.cache.got_artifact_failure(&digest) {
            if let Some(entry) = self.awaiting_layers.remove(&jid) {
                // If this was the first layer error for this request, then we'll find something in the
                // hash table, and we'll need to clean up.
                //
                // Otherwise, it means that there were previous errors for this entry, or it was
                // canceled, and there's nothing to do here.
                self.deps.send_message_to_broker(WorkerToBroker(
                    jid,
                    Err(JobError::System(format!(
                        "Failed to download and extract layer artifact {digest}: {err}"
                    ))),
                ));
                for digest in entry.tracker.into_digests() {
                    self.cache.decrement_ref_count(&digest);
                }
            }
        }
    }

    fn receive_artifact_success(&mut self, digest: Sha256Digest, bytes_used: u64) {
        let (path, jobs) = self.cache.got_artifact_success(&digest, bytes_used);
        for jid in jobs {
            match self.awaiting_layers.entry(jid) {
                Entry::Vacant(_) => {
                    // If there were previous errors for this job, or the job was canceled, then
                    // we'll find nothing in the hash table, and we'll need to release this layer.
                    self.cache.decrement_ref_count(&digest);
                }
                Entry::Occupied(mut entry) => {
                    // So far all is good. We then need to check if we've gotten all layers. If we
                    // have, then we can go ahead and schedule the job.
                    entry.get_mut().tracker.got(&digest, path.clone());
                    if entry.get().tracker.is_complete() {
                        let AwaitingLayersEntry { spec, tracker } = entry.remove();
                        self.enqueue_job_with_all_layers(jid, spec, tracker);
                    }
                }
            }
        }
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
    use itertools::Itertools;
    use maelstrom_test::*;
    use std::{cell::RefCell, rc::Rc, time::Duration};
    use BrokerToWorker::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        StartJob(JobId, JobSpec, Vec<PathBuf>),
        SendMessageToBroker(WorkerToBroker),
        StartArtifactFetch(Sha256Digest, ArtifactType, PathBuf),
        CacheGetArtifact(Sha256Digest, JobId),
        CacheGotArtifactSuccess(Sha256Digest, u64),
        CacheGotArtifactFailure(Sha256Digest),
        CacheDecrementRefCount(Sha256Digest),
        Kill(Pid),
        StartTimer(JobId, Duration),
        CancelTimer(JobId),
    }

    use TestMessage::*;

    struct TestState {
        messages: Vec<TestMessage>,
        start_job_returns: Vec<JobResult<Pid, String>>,
        get_artifact_returns: HashMap<Sha256Digest, GetArtifact>,
        got_artifact_success_returns: HashMap<Sha256Digest, (PathBuf, Vec<JobId>)>,
        got_artifact_failure_returns: HashMap<Sha256Digest, Vec<JobId>>,
    }

    impl DispatcherDeps for Rc<RefCell<TestState>> {
        fn start_job(
            &mut self,
            jid: JobId,
            spec: JobSpec,
            layers: NonEmpty<PathBuf>,
        ) -> (JobResult<Pid, String>, NonEmpty<Sha256Digest>) {
            let mut mut_ref = self.borrow_mut();
            let digest_layers = spec.layers.clone();
            mut_ref
                .messages
                .push(StartJob(jid, spec, Vec::from_iter(layers)));
            (
                mut_ref.start_job_returns.remove(0),
                digest_layers.map(|(d, _)| d),
            )
        }

        fn kill_job(&mut self, pid: Pid) {
            self.borrow_mut().messages.push(Kill(pid));
        }

        type TimerHandle = JobId;

        fn start_timer(&mut self, jid: JobId, duration: Duration) -> Self::TimerHandle {
            self.borrow_mut().messages.push(StartTimer(jid, duration));
            jid
        }

        fn cancel_timer(&mut self, handle: Self::TimerHandle) {
            self.borrow_mut().messages.push(CancelTimer(handle));
        }

        fn start_artifact_fetch(
            &mut self,
            digest: Sha256Digest,
            type_: ArtifactType,
            path: PathBuf,
        ) {
            self.borrow_mut()
                .messages
                .push(StartArtifactFetch(digest, type_, path));
        }

        fn send_message_to_broker(&mut self, message: WorkerToBroker) {
            self.borrow_mut()
                .messages
                .push(SendMessageToBroker(message));
        }
    }

    impl DispatcherCache for Rc<RefCell<TestState>> {
        fn get_artifact(&mut self, digest: Sha256Digest, jid: JobId) -> GetArtifact {
            eprintln!("{digest} {jid:?}");
            self.borrow_mut()
                .messages
                .push(CacheGetArtifact(digest.clone(), jid));
            self.borrow_mut()
                .get_artifact_returns
                .remove(&digest)
                .unwrap()
        }

        fn got_artifact_failure(&mut self, digest: &Sha256Digest) -> Vec<JobId> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactFailure(digest.clone()));
            self.borrow_mut()
                .got_artifact_failure_returns
                .remove(digest)
                .unwrap()
        }

        fn got_artifact_success(
            &mut self,
            digest: &Sha256Digest,
            bytes_used: u64,
        ) -> (PathBuf, Vec<JobId>) {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactSuccess(digest.clone(), bytes_used));
            self.borrow_mut()
                .got_artifact_success_returns
                .remove(digest)
                .unwrap()
        }

        fn decrement_ref_count(&mut self, digest: &Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefCount(digest.clone()))
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        dispatcher: Dispatcher<Rc<RefCell<TestState>>, Rc<RefCell<TestState>>>,
    }

    impl Fixture {
        fn new<const K: usize, const L: usize, const M: usize, const N: usize>(
            slots: u16,
            start_job_returns: [JobResult<Pid, String>; K],
            get_artifact_returns: [(Sha256Digest, GetArtifact); L],
            got_artifact_success_returns: [(Sha256Digest, (PathBuf, Vec<JobId>)); M],
            got_artifact_failure_returns: [(Sha256Digest, Vec<JobId>); N],
        ) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
                start_job_returns: Vec::from(start_job_returns),
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

        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let messages = &mut self.test_state.borrow_mut().messages;
            for perm in expected.clone().into_iter().permutations(expected.len()) {
                if perm == *messages {
                    messages.clear();
                    return;
                }
            }
            //            panic!(
            //                "Expected messages didn't match actual messages in any order.\n{}",
            //                colored_diff::PrettyDifference {
            //                    expected: &format!("{:#?}", expected),
            //                    actual: &format!("{:#?}", messages)
            //                }
            //            );
            panic!(
                "Expected messages didn't match actual messages in any order.\nExpected: {expected:#?}\nActual: {messages:#?}\n",
            );
        }
    }

    macro_rules! pid {
        ($n:expr) => {
            Pid::new_for_test($n)
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
            Ok(pid!(1)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
    }

    script_test! {
        enqueue_immediate_artifacts_no_error_slots_available,
        Fixture::new(1, [Ok(pid!(1))], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf_vec!["/a", "/b"])
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
    }

    script_test! {
        enqueue_mixed_artifacts_no_error_slots_available,
        Fixture::new(1, [
            Ok(pid!(1)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Get(path_buf!("/b"))),
            (digest!(43), GetArtifact::Wait),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (43, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartArtifactFetch(digest!(42), ArtifactType::Tar, path_buf!("/b")),
            CacheGetArtifact(digest!(43), jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(digest!(41)),
        };
    }

    script_test! {
        enqueue_immediate_artifacts_system_error_slots_available,
        Fixture::new(1, [
            Err(JobError::System(string!("se"))),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf_vec!["/a", "/b"]),
            SendMessageToBroker(WorkerToBroker(jid!(1), Err(JobError::System(string!("se"))))),
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        enqueue_immediate_artifacts_execution_error_slots_available,
        Fixture::new(1, [
            Err(JobError::Execution(string!("ee"))),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf_vec!["/a", "/b"]),
            SendMessageToBroker(WorkerToBroker(jid!(1), Err(JobError::Execution(string!("ee"))))),
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        enqueue_fills_all_slots,
        Fixture::new(4, [
            Ok(pid!(1)),
            Ok(pid!(2)),
            Ok(pid!(3)),
            Ok(pid!(4)),
            Ok(pid!(5)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
            (digest!(3), GetArtifact::Success(path_buf!("/c"))),
            (digest!(4), GetArtifact::Success(path_buf!("/d"))),
            (digest!(5), GetArtifact::Success(path_buf!("/e"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar))) => {
            CacheGetArtifact(digest!(3), jid!(3)),
            StartJob(jid!(3), spec!(3, Tar), path_buf_vec!["/c"]),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(digest!(4), jid!(4)),
            StartJob(jid!(4), spec!(4, Tar), path_buf_vec!["/d"]),
        };
        Broker(EnqueueJob(jid!(5), spec!(5, Tar))) => {
            CacheGetArtifact(digest!(5), jid!(5)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(5), spec!(5, Tar), path_buf_vec!["/e"]),
        }
    }

    script_test! {
        possibly_start_job_loops_until_slots_full,
        Fixture::new(1, [
            Ok(pid!(1)),
            Err(JobError::Execution(string!("ee"))),
            Err(JobError::System(string!("se"))),
            Ok(pid!(4)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
            (digest!(3), GetArtifact::Success(path_buf!("/c"))),
            (digest!(4), GetArtifact::Success(path_buf!("/d"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, Tar))) => {
            CacheGetArtifact(digest!(3), jid!(3)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(digest!(4), jid!(4)),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), outcome!(1))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
            SendMessageToBroker(WorkerToBroker(jid!(2), Err(JobError::Execution(string!("ee"))))),
            CacheDecrementRefCount(digest!(2)),
            StartJob(jid!(3), spec!(3, Tar), path_buf_vec!["/c"]),
            SendMessageToBroker(WorkerToBroker(jid!(3), Err(JobError::System(string!("se"))))),
            CacheDecrementRefCount(digest!(3)),
            StartJob(jid!(4), spec!(4, Tar), path_buf_vec!["/d"]),
        };
    }

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(1, [], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Wait),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(digest!(41)),
        };
    }

    script_test! {
        cancel_executing,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
            (digest!(43), GetArtifact::Success(path_buf!("/c"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf_vec!["/a", "/b"])
        };
        Broker(EnqueueJob(jid!(2), spec!(2, [(43, Tar)]))) => {
            CacheGetArtifact(digest!(43), jid!(2)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), spec!(2, [(43, Tar)]), path_buf_vec!["/c"]),
        };
    }

    script_test! {
        cancel_queued,
        Fixture::new(2, [
            Ok(pid!(1)),
            Ok(pid!(2)),
            Ok(pid!(4)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
            (digest!(4), GetArtifact::Success(path_buf!("/4"))),
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
            (digest!(43), GetArtifact::Success(path_buf!("/c"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/1"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
        Broker(EnqueueJob(jid!(3), spec!(3, [(41, Tar), (42, Tar), (43, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(3)),
            CacheGetArtifact(digest!(42), jid!(3)),
            CacheGetArtifact(digest!(43), jid!(3)),
        };
        Broker(EnqueueJob(jid!(4), spec!(4, Tar))) => {
            CacheGetArtifact(digest!(4), jid!(4)),
        };
        Broker(CancelJob(jid!(3))) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
            CacheDecrementRefCount(digest!(43)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), outcome!(1))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(4), spec!(4, Tar), path_buf_vec!["/4"]),
        };
    }

    script_test! {
        cancel_unknown,
        Fixture::new(1, [], [], [], []),
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_canceled,
        Fixture::new(1, [Ok(pid!(1))], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(CancelJob(jid!(1))) => { Kill(pid!(1)) };
        Broker(CancelJob(jid!(1))) => {};
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_timed_out,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf_vec!["/1"]),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobTimer(jid!(1)) => {
            Kill(pid!(1))
        };
        Broker(CancelJob(jid!(1))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
    }

    script_test! {
        receive_pid_status_unknown,
        Fixture::new(1, [], [], [], []),
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
    }

    script_test! {
        receive_pid_status_executing,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), outcome!(1))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        receive_pid_status_canceled,
        Fixture::new(1, [
            Ok(pid!(1)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), spec!(1, [(41, Tar), (42, Tar)]), path_buf_vec!["/a", "/b"]),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    #[test]
    #[should_panic(expected = "missing entry for JobId")]
    fn receive_job_stdout_unknown() {
        let mut fixture = Fixture::new(1, [], [], [], []);
        fixture
            .dispatcher
            .receive_message(JobStdout(jid!(1), Ok(JobOutputResult::None)));
    }

    #[test]
    #[should_panic(expected = "missing entry for JobId")]
    fn receive_job_stderr_unknown() {
        let mut fixture = Fixture::new(1, [], [], [], []);
        fixture
            .dispatcher
            .receive_message(JobStderr(jid!(1), Ok(JobOutputResult::None)));
    }

    script_test! {
        complete_stdout_stderr_status,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Signaled(9),
                effects: JobEffects {
                    stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                    stderr: JobOutputResult::Truncated {
                        first: boxed_u8!(b"stderr"),
                        truncated: 100,
                    },
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_stdout_status_stderr,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Signaled(9),
                effects: JobEffects {
                    stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                    stderr: JobOutputResult::Truncated {
                        first: boxed_u8!(b"stderr"),
                        truncated: 100,
                    },
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_stderr_stdout_status,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Signaled(9),
                effects: JobEffects {
                    stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                    stderr: JobOutputResult::Truncated {
                        first: boxed_u8!(b"stderr"),
                        truncated: 100,
                    },
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_stderr_status_stdout,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Signaled(9),
                effects: JobEffects {
                    stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                    stderr: JobOutputResult::Truncated {
                        first: boxed_u8!(b"stderr"),
                        truncated: 100,
                    },
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_status_stdout_stderr,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Signaled(9),
                effects: JobEffects {
                    stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                    stderr: JobOutputResult::Truncated {
                        first: boxed_u8!(b"stderr"),
                        truncated: 100,
                    },
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_status_stderr_stdout,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Signaled(9),
                effects: JobEffects {
                    stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                    stderr: JobOutputResult::Truncated {
                        first: boxed_u8!(b"stderr"),
                        truncated: 100,
                    },
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_bad_stdout,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Err(string!("stdout error"))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            SendMessageToBroker(
                WorkerToBroker(jid!(1), Err(JobError::System(string!("stdout error"))))
            ),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_bad_stderr,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        JobStderr(jid!(1), Err(string!("stderr error"))) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            SendMessageToBroker(
                WorkerToBroker(jid!(1), Err(JobError::System(string!("stderr error"))))
            ),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        complete_bad_stdout_and_stderr,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
            (digest!(2), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Err(string!("stdout error"))) => {};
        JobStderr(jid!(1), Err(string!("stderr error"))) => {};
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            SendMessageToBroker(
                WorkerToBroker(jid!(1), Err(JobError::System(string!("stdout error"))))
            ),
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/b"]),
        };
    }

    script_test! {
        timer_scheduled_then_canceled_on_success,
        Fixture::new(1, [
            Ok(pid!(1)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)), path_buf_vec!["/a"]),
            StartTimer(jid!(1), Duration::from_secs(33)),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                }
            }))),
            CacheDecrementRefCount(digest!(1)),
            CancelTimer(jid!(1)),
        };
    }

    script_test! {
        timer_scheduled_then_canceled_on_cancellation,
        Fixture::new(1, [
            Ok(pid!(1)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(33)), path_buf_vec!["/a"]),
            StartTimer(jid!(1), Duration::from_secs(33)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
            CancelTimer(jid!(1)),
        };
    }

    script_test! {
        time_out_running_1,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf_vec!["/1"]),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobTimer(jid!(1)) => {
            Kill(pid!(1)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::TimedOut(JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            })))),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
    }

    script_test! {
        time_out_running_2,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf_vec!["/1"]),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        JobTimer(jid!(1)) => {
            Kill(pid!(1)),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::TimedOut(JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            })))),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
    }

    script_test! {
        time_out_running_3,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf_vec!["/1"]),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobTimer(jid!(1)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stderr")))) => {
            CacheDecrementRefCount(digest!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::TimedOut(JobEffects {
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Inline(boxed_u8!(b"stderr")),
            })))),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
    }

    script_test! {
        time_out_completed,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf_vec!["/1"]),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {
            CacheDecrementRefCount(digest!(1)),
            CancelTimer(jid!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                }
            }))),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
        JobTimer(jid!(1)) => {};
    }

    script_test! {
        time_out_canceled,
        Fixture::new(1, [
            Ok(pid!(1)),
            Ok(pid!(2)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
            (digest!(2), GetArtifact::Success(path_buf!("/2"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, Tar).timeout(timeout!(1)), path_buf_vec!["/1"]),
            StartTimer(jid!(1), Duration::from_secs(1))
        };
        Broker(EnqueueJob(jid!(2), spec!(2, Tar))) => {
            CacheGetArtifact(digest!(2), jid!(2)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
            CancelTimer(jid!(1)),
        };
        JobTimer(jid!(1)) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(1)),
            StartJob(jid!(2), spec!(2, Tar), path_buf_vec!["/2"]),
        };
    }

    script_test! {
        error_cache_responses,
        Fixture::new(2, [], [
            (digest!(41), GetArtifact::Wait),
            (digest!(42), GetArtifact::Wait),
            (digest!(43), GetArtifact::Wait),
            (digest!(44), GetArtifact::Wait),
        ], [
            (digest!(41), (path_buf!("/a"), vec![jid!(1)])),
            (digest!(43), (path_buf!("/c"), vec![jid!(1)])),
        ], [
            (digest!(42), vec![jid!(1)]),
            (digest!(44), vec![jid!(1)]),
        ]),
        Broker(EnqueueJob(jid!(1), spec!(1, [(41, Tar), (42, Tar), (43, Tar), (44, Tar)]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            CacheGetArtifact(digest!(43), jid!(1)),
            CacheGetArtifact(digest!(44), jid!(1)),
        };
        ArtifactFetcher(digest!(41), Ok(101)) => {
            CacheGotArtifactSuccess(digest!(41), 101),
        };
        ArtifactFetcher(digest!(42), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(digest!(42)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Err(JobError::System(
                string!("Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a: foo"))))),
            CacheDecrementRefCount(digest!(41))
        };
        ArtifactFetcher(digest!(43), Ok(103)) => {
            CacheGotArtifactSuccess(digest!(43), 103),
            CacheDecrementRefCount(digest!(43))
        };
        ArtifactFetcher(digest!(44), Err(anyhow!("foo"))) => {
            CacheGotArtifactFailure(digest!(44)),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.is_none()")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(
            2,
            [Ok(pid!(1)), Ok(pid!(2))],
            [
                (digest!(1), GetArtifact::Success(path_buf!("/a"))),
                (digest!(2), GetArtifact::Success(path_buf!("/b"))),
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
            Ok(pid!(1)),
        ], [
            (digest!(1), GetArtifact::Success(path_buf!("/1"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), spec!(1, [(1, Tar), (1, Tar)]))) => {
            CacheGetArtifact(digest!(1), jid!(1)),
            StartJob(jid!(1), spec!(1, [(1, Tar), (1, Tar)]), path_buf_vec!["/1", "/1"]),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(1)),
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobOutcome::Completed {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                },
            }))),
        };
    }
}
