//! Central processing module for the worker. Receive messages from the broker, executors, and
//! artifact fetchers. Start or cancel jobs as appropriate via executors.

#![allow(dead_code)]

use crate::{
    cache::{Cache, CacheFs, GetArtifact},
    config::Slots,
};
use anyhow::{Error, Result};
use meticulous_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    JobDetails, JobId, JobOutputResult, JobResult, JobStatus, Sha256Digest,
};
use meticulous_util::ext::OptionExt as _;
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

/// Return value from [`DispatcherDeps::start_job`]. See [`JobResult`] for an explanation of the
/// [`StartJobResult::ExecutionError`] and [`StartJobResult::SystemError`] variants.
pub enum StartJobResult {
    Ok(Pid),
    ExecutionError(String),
    SystemError(String),
}

/// The external dependencies for [Dispatcher]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait DispatcherDeps {
    /// Start a new job. When the job terminates, the notification must come through as
    /// a [Message::Executor] message.
    fn start_job(
        &mut self,
        jid: JobId,
        details: &JobDetails,
        layers: Vec<PathBuf>,
    ) -> StartJobResult;

    /// Kill a running job.
    fn kill_job(&mut self, pid: Pid);

    /// Send a message to the broker.
    fn send_message_to_broker(&mut self, message: WorkerToBroker);

    /// Start a thread that will download an artifact from the broker and extract it into `path`.
    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf);
}

/// The [Cache] dependency for [Dispatcher]. This should be exactly the same as [Cache]'s public
/// interface. We have this so we can isolate [Dispatcher] when testing.
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

/// The standard implementation of [DispatcherCache] that just calls into [Cache].
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
    PidStatus(Pid, StdResult<JobStatus, String>),
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
            canceled: HashMap::default(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [Message] for more information.
    pub fn receive_message(&mut self, msg: Message) {
        match msg {
            Message::Broker(BrokerToWorker::EnqueueJob(jid, details)) => {
                self.receive_enqueue_job(jid, details)
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
    details: JobDetails,
    layers: HashMap<Sha256Digest, PathBuf>,
}

impl AwaitingLayersEntry {
    fn new(details: JobDetails) -> Self {
        AwaitingLayersEntry {
            details,
            layers: HashMap::default(),
        }
    }

    fn has_all_layers(&self) -> bool {
        self.layers.len() == self.details.layers.len()
    }
}

struct ExecutingJob {
    pid: Pid,
    status: Option<StdResult<JobStatus, String>>,
    stdout: Option<StdResult<JobOutputResult, String>>,
    stderr: Option<StdResult<JobOutputResult, String>>,
    layers: Vec<Sha256Digest>,
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
    queued: VecDeque<(JobId, JobDetails, Vec<PathBuf>)>,
    executing: HashMap<JobId, ExecutingJob>,
    canceled: HashMap<Pid, Vec<Sha256Digest>>,
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    fn possibly_start_job(&mut self) {
        while self.executing.len() < self.slots && !self.queued.is_empty() {
            let (jid, details, layer_paths) = self.queued.pop_front().unwrap();
            match self.deps.start_job(jid, &details, layer_paths) {
                StartJobResult::Ok(pid) => {
                    let executing_job = ExecutingJob {
                        pid,
                        status: None,
                        stdout: None,
                        stderr: None,
                        layers: details.layers,
                    };
                    self.executing.insert(jid, executing_job).assert_is_none();
                }
                StartJobResult::ExecutionError(e) => {
                    for digest in details.layers {
                        self.cache.decrement_ref_count(&digest);
                    }
                    self.deps
                        .send_message_to_broker(WorkerToBroker(jid, JobResult::ExecutionError(e)));
                }
                StartJobResult::SystemError(e) => {
                    for digest in details.layers {
                        self.cache.decrement_ref_count(&digest);
                    }
                    self.deps
                        .send_message_to_broker(WorkerToBroker(jid, JobResult::SystemError(e)));
                }
            }
        }
    }

    fn enqueue_job_with_all_layers(&mut self, jid: JobId, mut entry: AwaitingLayersEntry) {
        let layers = entry
            .details
            .layers
            .iter()
            .map(|digest| entry.layers.remove(digest).unwrap())
            .collect();
        self.queued.push_back((jid, entry.details, layers));
        self.possibly_start_job();
    }

    fn receive_enqueue_job(&mut self, jid: JobId, details: JobDetails) {
        let mut entry = AwaitingLayersEntry::new(details);
        for digest in &entry.details.layers {
            match self.cache.get_artifact(digest.clone(), jid) {
                GetArtifact::Success(path) => {
                    entry.layers.insert(digest.clone(), path).assert_is_none()
                }
                GetArtifact::Wait => {}
                GetArtifact::Get(path) => self.deps.start_artifact_fetch(digest.clone(), path),
            }
        }
        if entry.has_all_layers() {
            self.enqueue_job_with_all_layers(jid, entry);
        } else {
            self.awaiting_layers.insert(jid, entry).assert_is_none();
        }
    }

    fn receive_cancel_job(&mut self, jid: JobId) {
        let mut layers = vec![];
        if let Some(entry) = self.awaiting_layers.remove(&jid) {
            // We may have already gotten some layers. Make sure we release those.
            layers = entry.layers.into_keys().collect();
        } else if let Some(ExecutingJob { pid, layers, .. }) = self.executing.remove(&jid) {
            // The job was executing. We kill the job. We also start a new job if possible.
            // However, we wait to release the layers until the job has terminated. If we didn't it
            // would be possible for us to try to remove a directory that was still in use, which
            // would fail.
            self.deps.kill_job(pid);
            self.canceled.insert(pid, layers).assert_is_none();
            self.possibly_start_job();
        } else {
            // It may be the queue.
            self.queued.retain_mut(|x| {
                if x.0 != jid {
                    true
                } else {
                    assert!(layers.is_empty());
                    mem::swap(&mut x.1.layers, &mut layers);
                    false
                }
            });
        }
        for digest in layers {
            self.cache.decrement_ref_count(&digest);
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
        let entry = oe.get();
        if entry.status.is_some() && entry.stdout.is_some() && entry.stderr.is_some() {
            let (
                jid,
                ExecutingJob {
                    status: Some(status),
                    stdout: Some(stdout),
                    stderr: Some(stderr),
                    layers,
                    ..
                },
            ) = oe.remove_entry()
            else {
                panic!();
            };
            let result = match (status, stdout, stderr) {
                (StdResult::Ok(status), StdResult::Ok(stdout), StdResult::Ok(stderr)) => {
                    JobResult::Ran {
                        status,
                        stdout,
                        stderr,
                    }
                }
                (StdResult::Err(e), _, _)
                | (_, StdResult::Err(e), _)
                | (_, _, StdResult::Err(e)) => JobResult::SystemError(e),
            };
            self.deps
                .send_message_to_broker(WorkerToBroker(jid, result));
            for digest in layers {
                self.cache.decrement_ref_count(&digest);
            }
            self.possibly_start_job();
        }
    }

    fn receive_pid_status(&mut self, pid: Pid, status: StdResult<JobStatus, String>) {
        let target_jid =
            self.executing
                .iter()
                .find_map(|(jid, entry)| if entry.pid == pid { Some(*jid) } else { None });
        match target_jid {
            Some(jid) => {
                self.update_entry_and_potentially_finish_job(jid, move |entry| {
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
                    JobResult::SystemError(format!(
                        "Failed to download and extract layer artifact {digest}: {err}"
                    )),
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
    use super::Message::*;
    use super::*;
    use anyhow::anyhow;
    use itertools::Itertools;
    use meticulous_test::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use BrokerToWorker::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        StartJob(JobId, JobDetails, Vec<PathBuf>),
        SendMessageToBroker(WorkerToBroker),
        StartArtifactFetch(Sha256Digest, PathBuf),
        CacheGetArtifact(Sha256Digest, JobId),
        CacheGotArtifactSuccess(Sha256Digest, u64),
        CacheGotArtifactFailure(Sha256Digest),
        CacheDecrementRefCount(Sha256Digest),
        Kill(Pid),
    }

    use TestMessage::*;

    struct TestState {
        messages: Vec<TestMessage>,
        start_job_returns: Vec<StartJobResult>,
        get_artifact_returns: HashMap<Sha256Digest, GetArtifact>,
        got_artifact_success_returns: HashMap<Sha256Digest, (PathBuf, Vec<JobId>)>,
        got_artifact_failure_returns: HashMap<Sha256Digest, Vec<JobId>>,
    }

    impl DispatcherDeps for Rc<RefCell<TestState>> {
        fn start_job(
            &mut self,
            jid: JobId,
            details: &JobDetails,
            layers: Vec<PathBuf>,
        ) -> StartJobResult {
            let mut mut_ref = self.borrow_mut();
            mut_ref
                .messages
                .push(StartJob(jid, details.clone(), layers));
            mut_ref.start_job_returns.remove(0)
        }

        fn kill_job(&mut self, pid: Pid) {
            self.borrow_mut().messages.push(Kill(pid));
        }

        fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
            self.borrow_mut()
                .messages
                .push(StartArtifactFetch(digest, path));
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
                .remove(&digest)
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
                .remove(&digest)
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
            start_job_returns: [StartJobResult; K],
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
                "Expected messages didn't match actual messages in any order.\n\
                 Expected: {expected:#?}\nActual: {messages:#?}"
            );
        }
    }

    macro_rules! pid {
        ($n:expr) => {
            Pid::from_raw($n)
        };
    }

    macro_rules! boxed_u8 {
        ($n:literal) => {
            Vec::from(&$n[..]).into_boxed_slice()
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
        Fixture::new(1, [StartJobResult::Ok(pid!(1))], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
    }

    script_test! {
        enqueue_immediate_artifacts_no_error_slots_available,
        Fixture::new(1, [StartJobResult::Ok(pid!(1))], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42]), path_buf_vec!["/a", "/b"])
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
    }

    script_test! {
        enqueue_mixed_artifacts_no_error_slots_available,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Get(path_buf!("/b"))),
            (digest!(43), GetArtifact::Wait),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42, 43]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartArtifactFetch(digest!(42), path_buf!("/b")),
            CacheGetArtifact(digest!(43), jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(digest!(41)),
        };
    }

    script_test! {
        enqueue_immediate_artifacts_system_error_slots_available,
        Fixture::new(1, [
            StartJobResult::SystemError("se".to_string()),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42]), path_buf_vec!["/a", "/b"]),
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::SystemError("se".to_string()))),
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        enqueue_immediate_artifacts_execution_error_slots_available,
        Fixture::new(1, [
            StartJobResult::ExecutionError("ee".to_string()),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42]), path_buf_vec!["/a", "/b"]),
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::ExecutionError("ee".to_string()))),
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        enqueue_fills_all_slots,
        Fixture::new(4, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
            StartJobResult::Ok(pid!(3)),
            StartJobResult::Ok(pid!(4)),
            StartJobResult::Ok(pid!(5)),
        ], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
        Broker(EnqueueJob(jid!(3), details!(3))) => { StartJob(jid!(3), details!(3), vec![]) };
        Broker(EnqueueJob(jid!(4), details!(4))) => { StartJob(jid!(4), details!(4), vec![]) };
        Broker(EnqueueJob(jid!(5), details!(5))) => {};
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
            StartJob(jid!(5), details!(5), vec![]),
        };
    }

    script_test! {
        possibly_start_job_loops_until_slots_full,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::ExecutionError("ee".to_string()),
            StartJobResult::SystemError("se".to_string()),
            StartJobResult::ExecutionError("ee".to_string()),
            StartJobResult::SystemError("se".to_string()),
            StartJobResult::Ok(pid!(6)),
        ], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => {
            StartJob(jid!(1), details!(1), vec![]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        Broker(EnqueueJob(jid!(3), details!(3))) => {};
        Broker(EnqueueJob(jid!(4), details!(4))) => {};
        Broker(EnqueueJob(jid!(5), details!(5))) => {};
        Broker(EnqueueJob(jid!(6), details!(6))) => {};
        PidStatus(pid!(1), Ok(JobStatus::Exited(0))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            StartJob(jid!(2), details!(2), vec![]),
            SendMessageToBroker(WorkerToBroker(jid!(2), JobResult::ExecutionError("ee".to_string()))),
            StartJob(jid!(3), details!(3), vec![]),
            SendMessageToBroker(WorkerToBroker(jid!(3), JobResult::SystemError("se".to_string()))),
            StartJob(jid!(4), details!(4), vec![]),
            SendMessageToBroker(WorkerToBroker(jid!(4), JobResult::ExecutionError("ee".to_string()))),
            StartJob(jid!(5), details!(5), vec![]),
            SendMessageToBroker(WorkerToBroker(jid!(5), JobResult::SystemError("se".to_string()))),
            StartJob(jid!(6), details!(6), vec![]),
        };
    }

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(1, [], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Wait),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42]))) => {
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
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
            (digest!(43), GetArtifact::Success(path_buf!("/c"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42]), path_buf_vec!["/a", "/b"])
        };
        Broker(EnqueueJob(jid!(2), details!(2, [43]))) => {
            CacheGetArtifact(digest!(43), jid!(2)),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
            StartJob(jid!(2), details!(2, [43]), path_buf_vec!["/c"]),
        };
        PidStatus(pid!(1), Ok(JobStatus::Exited(0))) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        cancel_queued,
        Fixture::new(2, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
            StartJobResult::Ok(pid!(4)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
            (digest!(43), GetArtifact::Success(path_buf!("/c"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
        Broker(EnqueueJob(jid!(3), details!(3, [41, 42, 43]))) => {
            CacheGetArtifact(digest!(41), jid!(3)),
            CacheGetArtifact(digest!(42), jid!(3)),
            CacheGetArtifact(digest!(43), jid!(3)),
        };
        Broker(EnqueueJob(jid!(4), details!(4))) => {};
        Broker(CancelJob(jid!(3))) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
            CacheDecrementRefCount(digest!(43)),
        };
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), Ok(JobStatus::Exited(0))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            StartJob(jid!(4), details!(4), vec![]),
        };
    }

    script_test! {
        cancel_unknown,
        Fixture::new(1, [], [], [], []),
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        cancel_canceled,
        Fixture::new(1, [StartJobResult::Ok(pid!(1))], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(CancelJob(jid!(1))) => { Kill(pid!(1)) };
        Broker(CancelJob(jid!(1))) => {};
        Broker(CancelJob(jid!(1))) => {};
    }

    script_test! {
        receive_pid_status_unknown,
        Fixture::new(1, [], [], [], []),
        PidStatus(pid!(1), Ok(JobStatus::Exited(0))) => {};
    }

    script_test! {
        receive_pid_status_executing,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => {
            StartJob(jid!(1), details!(1), vec![]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::None)) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::None)) => {};
        PidStatus(pid!(1), Ok(JobStatus::Exited(0))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            StartJob(jid!(2), details!(2), vec![]),
        };
    }

    script_test! {
        receive_pid_status_canceled,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42]), path_buf_vec!["/a", "/b"]),
        };
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
        };
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {
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
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(42), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) => {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
            })),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), details!(2), vec![]),
        };
    }

    script_test! {
        complete_stdout_status_stderr,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(42), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) => {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
            })),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), details!(2), vec![]),
        };
    }

    script_test! {
        complete_stderr_stdout_status,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(42), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) => {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
            })),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), details!(2), vec![]),
        };
    }

    script_test! {
        complete_stderr_status_stdout,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(42), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) => {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
            })),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), details!(2), vec![]),
        };
    }

    script_test! {
        complete_status_stdout_stderr,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(42), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) => {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
            })),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), details!(2), vec![]),
        };
    }

    script_test! {
        complete_status_stderr_stdout,
        Fixture::new(1, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [
            (digest!(42), GetArtifact::Success(path_buf!("/a"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) => {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]),
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => {};
        PidStatus(pid!(1), Ok(JobStatus::Signalled(9))) => {};
        JobStderr(jid!(1), Ok(JobOutputResult::Truncated {
            first: boxed_u8!(b"stderr"),
            truncated: 100,
        })) => {};
        JobStdout(jid!(1), Ok(JobOutputResult::Inline(boxed_u8!(b"stdout")))) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::Inline(boxed_u8!(b"stdout")),
                stderr: JobOutputResult::Truncated {
                    first: boxed_u8!(b"stderr"),
                    truncated: 100,
                },
            })),
            CacheDecrementRefCount(digest!(42)),
            StartJob(jid!(2), details!(2), vec![]),
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
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42, 43, 44]))) => {
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
            SendMessageToBroker(WorkerToBroker(jid!(1), JobResult::SystemError(
                "Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a: foo".to_string()))),
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
        let mut fixture = Fixture::new(2, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
        ], [], [], []);
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), details!(1))));
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), details!(2))));
    }
}
