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
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

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
    JobStdout(JobId, JobOutputResult),
    JobStderr(JobId, JobOutputResult),
    PidStatus(Pid, JobStatus),
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
            Message::JobStdout(jid, result) => self.receive_job_stdout(jid, result),
            Message::JobStderr(jid, result) => self.receive_job_stderr(jid, result),
            Message::PidStatus(pid, status) => self.receive_pid_status(pid, status),
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
    status: Option<std::result::Result<JobStatus, String>>,
    stdout: Option<std::result::Result<JobOutputResult, String>>,
    stderr: Option<std::result::Result<JobOutputResult, String>>,
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
                    self.deps
                        .send_message_to_broker(WorkerToBroker(jid, JobResult::ExecutionError(e)));
                }
                StartJobResult::SystemError(e) => {
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

    fn receive_job_stdout(&mut self, jid: JobId, result: JobOutputResult) {
        if let Entry::Occupied(mut oe) = self.executing.entry(jid) {
            let entry = oe.get_mut();
            entry.stdout = Some(Ok(result));
            if entry.status.is_some() && entry.stderr.is_some() {
                let (
                    _,
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
                    (Ok(status), Ok(stdout), Ok(stderr)) => JobResult::Ran {
                        status,
                        stdout,
                        stderr,
                    },
                    (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => JobResult::SystemError(e),
                };
                self.deps
                    .send_message_to_broker(WorkerToBroker(jid, result));
                for digest in layers {
                    self.cache.decrement_ref_count(&digest);
                }
                self.possibly_start_job();
            }
        }
    }

    fn receive_job_stderr(&mut self, jid: JobId, result: JobOutputResult) {
        if let Entry::Occupied(mut oe) = self.executing.entry(jid) {
            let entry = oe.get_mut();
            entry.stderr = Some(Ok(result));
            if entry.status.is_some() && entry.stdout.is_some() {
                let (
                    _,
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
                    (Ok(status), Ok(stdout), Ok(stderr)) => JobResult::Ran {
                        status,
                        stdout,
                        stderr,
                    },
                    (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => JobResult::SystemError(e),
                };
                self.deps
                    .send_message_to_broker(WorkerToBroker(jid, result));
                for digest in layers {
                    self.cache.decrement_ref_count(&digest);
                }
                self.possibly_start_job();
            }
        }
    }

    fn receive_pid_status(&mut self, pid: Pid, status: JobStatus) {
        let mut target_jid = Option::<JobId>::None;
        for (jid, entry) in self.executing.iter() {
            if entry.pid == pid {
                target_jid = Some(*jid);
                break;
            }
        }
        match target_jid {
            Some(jid) => {
                let Entry::Occupied(mut oe) = self.executing.entry(jid) else {
                    panic!()
                };
                let entry = oe.get_mut();
                entry.status = Some(Ok(status));
                if entry.stdout.is_some() && entry.stderr.is_some() {
                    let (
                        _,
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
                        (Ok(status), Ok(stdout), Ok(stderr)) => JobResult::Ran {
                            status,
                            stdout,
                            stderr,
                        },
                        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                            JobResult::SystemError(e)
                        }
                    };
                    self.deps
                        .send_message_to_broker(WorkerToBroker(jid, result));
                    for digest in layers {
                        self.cache.decrement_ref_count(&digest);
                    }
                    self.possibly_start_job();
                }
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

    fn receive_artifact_failure(&mut self, digest: Sha256Digest, err: Error) {
        for jid in self.cache.got_artifact_failure(&digest) {
            // If this was the first layer error for this request, then we'll find something in the
            // hash table, and we'll need to clean up.
            //
            // Otherwise, it means that there were previous errors for this entry, and there's
            // nothing to do here.
            if let Some(entry) = self.awaiting_layers.remove(&jid) {
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
            // If there were previous errors for this job, then we'll find
            // nothing in the hash table, and we'll need to release this layer.
            //
            // Otherwise, it means that so far all is good. We then need to check if
            // we've gotten all layers. If we have, then we can go ahead and schedule
            // the job.
            match self.awaiting_layers.entry(jid) {
                Entry::Vacant(_) => {
                    self.cache.decrement_ref_count(&digest);
                }
                Entry::Occupied(mut entry) => {
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
        enqueue_1,
        Fixture::new(2, [StartJobResult::Ok(pid!(1))], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
    }

    script_test! {
        enqueue_2,
        Fixture::new(2, [StartJobResult::Ok(pid!(1)), StartJobResult::Ok(pid!(2))], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
    }

    script_test! {
        enqueue_3_with_2_slots,
        Fixture::new(2, [StartJobResult::Ok(pid!(1)), StartJobResult::Ok(pid!(2))], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
        Broker(EnqueueJob(jid!(3), details!(3))) => {};
    }

    script_test! {
        enqueue_1_with_layers_all_available,
        Fixture::new(2, [
            StartJobResult::Ok(pid!(1)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
            (digest!(43), GetArtifact::Success(path_buf!("/c")))
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42, 43]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            CacheGetArtifact(digest!(43), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42, 43]), path_buf_vec!["/a", "/b", "/c"])
        };
    }

    script_test! {
        enqueue_1_with_layers_some_available,
        Fixture::new(
            2, [
                StartJobResult::Ok(pid!(1)),
            ], [
                (digest!(41), GetArtifact::Success(path_buf!("/a"))),
                (digest!(42), GetArtifact::Wait),
                (digest!(43), GetArtifact::Get(path_buf!("/c")))
            ], [
                (digest!(42), (path_buf!("/b"), vec![jid!(1)])),
                (digest!(43), (path_buf!("/c"), vec![jid!(1)])),
            ], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42, 43]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            CacheGetArtifact(digest!(43), jid!(1)),
            StartArtifactFetch(digest!(43), path_buf!("/c")),
        };

        ArtifactFetcher(digest!(42), Ok(100)) => {
            CacheGotArtifactSuccess(digest!(42), 100),
        };
        ArtifactFetcher(digest!(43), Ok(100)) => {
            CacheGotArtifactSuccess(digest!(43), 100),
            StartJob(jid!(1), details!(1, [41, 42, 43]), path_buf_vec!["/a", "/b", "/c"])
        }
    }

    script_test! {
        complete_1_stdout_stderr_status,
        Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1))],
            [(digest!(42), GetArtifact::Success(path_buf!("/a")))],
            [],
            []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) =>  {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]) };
        JobStdout(jid!(1), JobOutputResult::None) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        complete_1_stdout_status_stderr,
        Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1))],
            [(digest!(42), GetArtifact::Success(path_buf!("/a")))],
            [],
            []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) =>  {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]) };
        JobStdout(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        complete_1_stderr_stdout_status,
        Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1))],
            [(digest!(42), GetArtifact::Success(path_buf!("/a")))],
            [],
            []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) =>  {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]) };
        JobStderr(jid!(1), JobOutputResult::None) => {};
        JobStdout(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        complete_1_stderr_status_stdout,
        Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1))],
            [(digest!(42), GetArtifact::Success(path_buf!("/a")))],
            [],
            []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) =>  {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]) };
        JobStderr(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStdout(jid!(1), JobOutputResult::None) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        complete_1_status_stdout_stderr,
        Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1))],
            [(digest!(42), GetArtifact::Success(path_buf!("/a")))],
            [],
            []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) =>  {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]) };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStdout(jid!(1), JobOutputResult::None) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        complete_1_status_stderr_stdout,
        Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1))],
            [(digest!(42), GetArtifact::Success(path_buf!("/a")))],
            [],
            []),
        Broker(EnqueueJob(jid!(1), details!(1, [42]))) =>  {
            CacheGetArtifact(digest!(42), jid!(1)),
            StartJob(jid!(1), details!(1, [42]), path_buf_vec!["/a"]) };
        PidStatus(pid!(1), JobStatus::Exited(0)) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {};
        JobStdout(jid!(1), JobOutputResult::None) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            CacheDecrementRefCount(digest!(42)),
        };
    }

    script_test! {
        complete_1_while_blocked,
        Fixture::new(2, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
            StartJobResult::Ok(pid!(3)),
        ], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
        Broker(EnqueueJob(jid!(3), details!(3))) => {};
        JobStdout(jid!(1), JobOutputResult::None) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            StartJob(jid!(3), details!(3), vec![]),
        };
    }

    script_test! {
        enqueue_2_complete_1_enqueue_1,
        Fixture::new(2, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
            StartJobResult::Ok(pid!(3)),
        ], [], [], []),
        Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
        JobStdout(jid!(1), JobOutputResult::None) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1)))
        };
        Broker(EnqueueJob(jid!(3), details!(3))) => { StartJob(jid!(3), details!(3), vec![]) };
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
        JobStdout(jid!(1), JobOutputResult::None) => {};
        JobStderr(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            SendMessageToBroker(WorkerToBroker(jid!(1), result!(1))),
            StartJob(jid!(4), details!(4), vec![]),
        };
    }

    script_test! {
        cancel_executing,
        Fixture::new(2, [
            StartJobResult::Ok(pid!(1)),
            StartJobResult::Ok(pid!(2)),
            StartJobResult::Ok(pid!(3)),
        ], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Success(path_buf!("/b"))),
            (digest!(43), GetArtifact::Success(path_buf!("/c"))),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42, 43]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            CacheGetArtifact(digest!(43), jid!(1)),
            StartJob(jid!(1), details!(1, [41, 42, 43]), path_buf_vec!["/a", "/b", "/c"])
        };
        Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
        Broker(EnqueueJob(jid!(3), details!(3))) => {};
        Broker(CancelJob(jid!(1))) => {
            Kill(pid!(1)),
            StartJob(jid!(3), details!(3), vec![])
        };
        JobStdout(jid!(1), JobOutputResult::None) => {};
        PidStatus(pid!(1), JobStatus::Exited(0)) => {
            CacheDecrementRefCount(digest!(41)),
            CacheDecrementRefCount(digest!(42)),
            CacheDecrementRefCount(digest!(43)),
        };
        JobStderr(jid!(1), JobOutputResult::None) => {};
    }

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(2, [], [
            (digest!(41), GetArtifact::Success(path_buf!("/a"))),
            (digest!(42), GetArtifact::Wait),
            (digest!(43), GetArtifact::Wait),
        ], [], []),
        Broker(EnqueueJob(jid!(1), details!(1, [41, 42, 43]))) => {
            CacheGetArtifact(digest!(41), jid!(1)),
            CacheGetArtifact(digest!(42), jid!(1)),
            CacheGetArtifact(digest!(43), jid!(1)),
        };
        Broker(CancelJob(jid!(1))) => {
            CacheDecrementRefCount(digest!(41))
        }
    }

    script_test! {
            cancels_idempotent,
            Fixture::new(2, [
                StartJobResult::Ok(pid!(1)),
                StartJobResult::Ok(pid!(2)),
                StartJobResult::Ok(pid!(3)),
    ], [], [], []),
            Broker(CancelJob(jid!(2))) => {};
            Broker(CancelJob(jid!(2))) => {};
            Broker(EnqueueJob(jid!(1), details!(1))) => { StartJob(jid!(1), details!(1), vec![]) };
            Broker(EnqueueJob(jid!(2), details!(2))) => { StartJob(jid!(2), details!(2), vec![]) };
            Broker(EnqueueJob(jid!(3), details!(3))) => {};
            Broker(EnqueueJob(jid!(4), details!(4))) => {};
            Broker(CancelJob(jid!(4))) => {};
            Broker(CancelJob(jid!(4))) => {};
            Broker(CancelJob(jid!(4))) => {};
            Broker(CancelJob(jid!(2))) => {
                Kill(pid!(2)),
                StartJob(jid!(3), details!(3), vec![]),
            };
            Broker(CancelJob(jid!(2))) => {};
            Broker(CancelJob(jid!(2))) => {};
            PidStatus(pid!(2), JobStatus::Exited(1)) => {};
            Broker(CancelJob(jid!(2))) => {};
            Broker(CancelJob(jid!(2))) => {};
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
        let mut fixture = Fixture::new(
            2,
            [StartJobResult::Ok(pid!(1)), StartJobResult::Ok(pid!(2))],
            [],
            [],
            [],
        );
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), details!(1))));
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid!(1), details!(2))));
    }
}
