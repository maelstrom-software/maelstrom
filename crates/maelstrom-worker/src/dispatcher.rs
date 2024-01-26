//! Central processing module for the worker. Receive messages from the broker, executors, and
//! artifact fetchers. Start or cancel jobs as appropriate via executors.

use crate::{
    cache::{Cache, CacheFs, GetArtifact},
    config::Slots,
};
use anyhow::{Error, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    ArtifactType, JobError, JobId, JobOutputResult, JobResult, JobSpec, JobStatus, JobSuccess,
    NonEmpty, Sha256Digest,
};
use maelstrom_util::ext::OptionExt as _;
use nix::unistd::Pid;
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    mem,
    path::PathBuf,
    result::Result as StdResult,
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// The external dependencies for [Dispatcher]. All of these methods must be asynchronous: they
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

    /// Kill a running job using the [`Pid`] obtained from [`JobResult::Ok`].
    fn kill_job(&mut self, pid: Pid);

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
            canceled: HashMap::default(),
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
    layers: HashMap<Sha256Digest, PathBuf>,
}

impl AwaitingLayersEntry {
    fn new(spec: JobSpec) -> Self {
        AwaitingLayersEntry {
            spec,
            layers: HashMap::default(),
        }
    }

    fn has_all_layers(&self) -> bool {
        self.layers.len() == self.spec.layers.len()
    }
}

struct ExecutingJob {
    pid: Pid,
    status: Option<JobStatus>,
    stdout: Option<StdResult<JobOutputResult, String>>,
    stderr: Option<StdResult<JobOutputResult, String>>,
    layers: NonEmpty<Sha256Digest>,
}

impl ExecutingJob {
    fn new(pid: Pid, layers: NonEmpty<Sha256Digest>) -> Self {
        ExecutingJob {
            pid,
            status: None,
            stdout: None,
            stderr: None,
            layers,
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
    queued: VecDeque<(JobId, JobSpec, NonEmpty<PathBuf>)>,
    executing: HashMap<JobId, ExecutingJob>,
    executing_pids: HashMap<Pid, JobId>,
    canceled: HashMap<Pid, NonEmpty<Sha256Digest>>,
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    fn possibly_start_job(&mut self) {
        while self.executing.len() < self.slots && !self.queued.is_empty() {
            let (jid, spec, layer_paths) = self.queued.pop_front().unwrap();
            match self.deps.start_job(jid, spec, layer_paths) {
                (Ok(pid), layers) => {
                    let executing_job = ExecutingJob::new(pid, layers);
                    self.executing.insert(jid, executing_job).assert_is_none();
                    self.executing_pids.insert(pid, jid).assert_is_none();
                }
                (Err(e), layers) => {
                    for digest in layers {
                        self.cache.decrement_ref_count(&digest);
                    }
                    self.deps
                        .send_message_to_broker(WorkerToBroker(jid, Err(e)));
                }
            }
        }
    }

    fn enqueue_job_with_all_layers(&mut self, jid: JobId, mut entry: AwaitingLayersEntry) {
        let layers = NonEmpty::<PathBuf>::collect(
            entry
                .spec
                .layers
                .iter()
                .map(|(digest, _)| entry.layers.remove(digest).unwrap()),
        )
        .unwrap();
        self.queued.push_back((jid, entry.spec, layers));
        self.possibly_start_job();
    }

    fn receive_enqueue_job(&mut self, jid: JobId, spec: JobSpec) {
        let mut entry = AwaitingLayersEntry::new(spec);
        for (digest, type_) in &entry.spec.layers {
            match self.cache.get_artifact(digest.clone(), jid) {
                GetArtifact::Success(path) => {
                    entry.layers.insert(digest.clone(), path).assert_is_none()
                }
                GetArtifact::Wait => {}
                GetArtifact::Get(path) => {
                    self.deps.start_artifact_fetch(digest.clone(), *type_, path)
                }
            }
        }
        if entry.has_all_layers() {
            self.enqueue_job_with_all_layers(jid, entry);
        } else {
            self.awaiting_layers.insert(jid, entry).assert_is_none();
        }
    }

    fn receive_cancel_job(&mut self, jid: JobId) {
        let mut layers = None;
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // We may have already gotten some layers. Make sure we release those.
            layers = NonEmpty::collect(entry.layers.into_keys());
        } else if let Some(ExecutingJob { pid, layers, .. }) = self.executing.remove(&jid) {
            // The job was executing. We kill the job. We also start a new job if possible.
            // However, we wait to release the layers until the job has terminated. If we didn't it
            // would be possible for us to try to remove a directory that was still in use, which
            // would fail.
            self.executing_pids.remove(&pid).assert_is_some();
            self.deps.kill_job(pid);
            self.canceled.insert(pid, layers).assert_is_none();
            self.possibly_start_job();
        } else {
            // It may be the queue.
            self.queued = mem::take(&mut self.queued)
                .into_iter()
                .filter_map(|x| {
                    if x.0 != jid {
                        Some(x)
                    } else {
                        assert!(layers.is_none());
                        layers = Some(x.1.layers.map(|(d, _)| d));
                        None
                    }
                })
                .collect();
        }
        if let Some(layers) = layers {
            for digest in layers {
                self.cache.decrement_ref_count(&digest);
            }
        }
    }

    fn update_entry_and_potentially_finish_job(
        &mut self,
        jid: JobId,
        f: impl FnOnce(&mut ExecutingJob),
    ) {
        let Entry::Occupied(mut oe) = self.executing.entry(jid) else {
            return;
        };
        f(oe.get_mut());
        if oe.get().is_complete() {
            let (jid, entry) = oe.remove_entry();
            self.executing_pids.remove(&entry.pid).assert_is_some();
            let status = entry.status.unwrap();
            let stdout = entry.stdout.unwrap();
            let stderr = entry.stderr.unwrap();
            let result = match (status, stdout, stderr) {
                (status, StdResult::Ok(stdout), StdResult::Ok(stderr)) => Ok(JobSuccess {
                    status,
                    stdout,
                    stderr,
                }),
                (_, StdResult::Err(e), _) | (_, _, StdResult::Err(e)) => Err(JobError::System(e)),
            };
            self.deps
                .send_message_to_broker(WorkerToBroker(jid, result));
            for digest in entry.layers {
                self.cache.decrement_ref_count(&digest);
            }
            self.possibly_start_job();
        }
    }

    fn receive_pid_status(&mut self, pid: Pid, status: JobStatus) {
        let target_jid = self.executing_pids.get(&pid);
        match target_jid {
            Some(jid) => {
                self.update_entry_and_potentially_finish_job(*jid, move |entry| {
                    entry.status = Some(status)
                });
            }
            None => {
                if let Some(layers) = self.canceled.remove(&pid) {
                    for digest in layers {
                        self.cache.decrement_ref_count(&digest);
                    }
                }
            }
        }
    }

    fn receive_job_stdout(&mut self, jid: JobId, result: StdResult<JobOutputResult, String>) {
        self.update_entry_and_potentially_finish_job(jid, move |entry| entry.stdout = Some(result));
    }

    fn receive_job_stderr(&mut self, jid: JobId, result: StdResult<JobOutputResult, String>) {
        self.update_entry_and_potentially_finish_job(jid, move |entry| entry.stderr = Some(result));
    }

    fn receive_artifact_failure(&mut self, digest: Sha256Digest, err: Error) {
        for jid in self.cache.got_artifact_failure(&digest) {
            if let Some(entry) = self.awaiting_layers.remove(&jid) {
                // If this was the first layer error for this request, then we'll find something in the
                // hash table, and we'll need to clean up.
                //
                // Otherwise, it means that there were previous errors for this entry, and there's
                // nothing to do here.
                self.deps.send_message_to_broker(WorkerToBroker(
                    jid,
                    Err(JobError::System(format!(
                        "Failed to download and extract layer artifact {digest}: {err}"
                    ))),
                ));
                for digest in entry.layers.into_keys() {
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
                    // If there were previous errors for this job, then we'll find
                    // nothing in the hash table, and we'll need to release this layer.
                    self.cache.decrement_ref_count(&digest);
                }
                Entry::Occupied(mut entry) => {
                    // So far all is good. We then need to check if we've gotten all layers. If we
                    // have, then we can go ahead and schedule the job.
                    entry
                        .get_mut()
                        .layers
                        .insert(digest.clone(), path.clone())
                        .assert_is_none();
                    if entry.get().has_all_layers() {
                        let entry = entry.remove();
                        self.enqueue_job_with_all_layers(jid, entry);
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
    use std::{cell::RefCell, rc::Rc};
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
            panic!(
                "Expected messages didn't match actual messages in any order.\n{}",
                colored_diff::PrettyDifference {
                    expected: &format!("{:#?}", expected),
                    actual: &format!("{:#?}", messages)
                }
            );
        }
    }

    macro_rules! pid {
        ($n:expr) => {
            Pid::from_raw($n)
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
            StartJob(jid!(5), spec!(5, Tar), path_buf_vec!["/e"]),
        };
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
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
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
            StartJob(jid!(2), spec!(2, [(43, Tar)]), path_buf_vec!["/c"]),
        };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
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
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
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
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
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
        PidStatus(pid!(1), JobStatus::Signaled(9)) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        receive_job_stdout_unknown,
        Fixture::new(1, [], [], [], []),
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
    }

    script_test! {
        receive_job_stderr_unknown,
        Fixture::new(1, [], [], [], []),
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
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
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobSuccess {
                status: JobStatus::Signaled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
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
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobSuccess {
                status: JobStatus::Signaled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
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
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobSuccess {
                status: JobStatus::Signaled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
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
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobSuccess {
                status: JobStatus::Signaled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
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
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobSuccess {
                status: JobStatus::Signaled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
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
            SendMessageToBroker(WorkerToBroker(jid!(1), Ok(JobSuccess {
                status: JobStatus::Signaled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
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
}
