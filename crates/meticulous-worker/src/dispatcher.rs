//! Central processing module for the worker. Receive messages from the broker and executors, and
//! start or cancel jobs as appropriate.

use super::cache::{Cache, CacheFs, GetArtifact};
use meticulous_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    JobDetails, JobId, JobResult, Sha256Digest,
};
use meticulous_util::OptionExt;
use std::{
    collections::{hash_map, HashMap, VecDeque},
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

/// The external dependencies for [Dispatcher]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait DispatcherDeps {
    /// A handle to a pending job. When dropped, the handle must tell the job to
    /// terminate immediately. However, the drop method must not wait for the job to
    /// terminate. Instead, that must happen asynchronously, and the actual job termination
    /// notification must come through as a [Message::Executor] message.
    ///
    /// It must be safe to drop the handle after the job has terminated.
    type JobHandle;

    /// Start a new job. When the job terminates, the notification must come through as
    /// a [Message::Executor] message.
    fn start_job(
        &mut self,
        id: JobId,
        details: &JobDetails,
        layers: Vec<PathBuf>,
    ) -> Self::JobHandle;

    /// Start a thread that will download an artifact from the broker and extract it into `path`.
    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf);

    /// Send a message to the broker.
    fn send_response_to_broker(&mut self, message: WorkerToBroker);
}

pub trait DispatcherCache {
    fn get_artifact(&mut self, jid: JobId, artifact: Sha256Digest) -> GetArtifact;
    fn got_artifact_failure(&mut self, digest: Sha256Digest) -> Vec<JobId>;
    fn got_artifact_success(
        &mut self,
        digest: Sha256Digest,
        bytes_used: u64,
    ) -> Vec<(JobId, PathBuf)>;
    fn decrement_refcount(&mut self, digest: Sha256Digest);
}

impl<FsT: CacheFs> DispatcherCache for Cache<FsT> {
    fn get_artifact(&mut self, jid: JobId, artifact: Sha256Digest) -> GetArtifact {
        self.get_artifact(jid, artifact)
    }

    fn got_artifact_failure(&mut self, digest: Sha256Digest) -> Vec<JobId> {
        self.got_artifact_failure(digest)
    }

    fn got_artifact_success(
        &mut self,
        digest: Sha256Digest,
        bytes_used: u64,
    ) -> Vec<(JobId, PathBuf)> {
        self.got_artifact_success(digest, bytes_used)
    }

    fn decrement_refcount(&mut self, digest: Sha256Digest) {
        self.decrement_refcount(digest)
    }
}

/// An input message for the dispatcher. These come from either the broker or from an executor.
#[derive(Debug)]
pub enum Message {
    Broker(BrokerToWorker),
    Executor(JobId, JobResult),
    #[allow(dead_code)]
    ArtifactFetcher(Sha256Digest, Option<u64>),
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    /// Create a new dispatcher with the provided slot count. The slot count must be a positive
    /// number.
    pub fn new(deps: DepsT, cache: CacheT, slots: usize) -> Self {
        assert!(slots > 0);
        Dispatcher {
            deps,
            cache,
            slots,
            awaiting_layers: HashMap::default(),
            queued: VecDeque::default(),
            executing: HashMap::default(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [Message] for more information.
    pub fn receive_message(&mut self, msg: Message) {
        match msg {
            Message::Broker(BrokerToWorker::EnqueueJob(id, details)) => {
                self.receive_enqueue_job(id, details)
            }
            Message::Broker(BrokerToWorker::CancelJob(id)) => self.receive_cancel_job(id),
            Message::Executor(id, result) => self.receive_job_result(id, result),
            Message::ArtifactFetcher(digest, None) => self.receive_artifact_failure(digest),
            Message::ArtifactFetcher(digest, Some(bytes_used)) => {
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
            layers: HashMap::new(),
        }
    }

    fn has_all_layers(&self) -> bool {
        self.layers.len() == self.details.layers.len()
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
    queued: VecDeque<(JobId, JobDetails, Vec<PathBuf>)>,
    executing: HashMap<JobId, (JobDetails, DepsT::JobHandle)>,
}

impl<DepsT: DispatcherDeps, CacheT: DispatcherCache> Dispatcher<DepsT, CacheT> {
    fn possibly_start_job(&mut self) {
        if self.executing.len() < self.slots {
            if let Some((id, details, layer_paths)) = self.queued.pop_front() {
                let handle = self.deps.start_job(id, &details, layer_paths);
                assert!(self.executing.insert(id, (details, handle)).is_none());
            }
        }
    }

    fn receive_enqueue_job(&mut self, id: JobId, details: JobDetails) {
        let mut entry = AwaitingLayersEntry::new(details);
        for digest in &entry.details.layers {
            match self.cache.get_artifact(id, digest.clone()) {
                GetArtifact::Success(path) => {
                    entry.layers.insert(digest.clone(), path).assert_is_none()
                }
                GetArtifact::Wait => {}
                GetArtifact::Get(path) => self.deps.start_artifact_fetch(digest.clone(), path),
            }
        }
        if entry.has_all_layers() {
            let layers = entry
                .details
                .layers
                .iter()
                .map(|digest| entry.layers.remove(digest).unwrap())
                .collect();
            self.queued.push_back((id, entry.details, layers));
            self.possibly_start_job();
        } else {
            self.awaiting_layers.insert(id, entry).assert_is_none();
        }
    }

    fn receive_cancel_job(&mut self, id: JobId) {
        let mut layers = vec![];
        if let Some(entry) = self.awaiting_layers.remove(&id) {
            // We may have already gotten some layers. Make sure we release those.
            layers = entry.layers.into_keys().collect();
        } else if let Some((details, _)) = self.executing.remove(&id) {
            // If it was executing, we need to drop the job handle, which will
            // tell the executor to kill the process. We also need to clean up all of our layers.
            layers = details.layers;
            self.possibly_start_job();
        } else {
            // It may be the queue.
            self.queued.retain_mut(|x| {
                if x.0 != id {
                    true
                } else {
                    assert!(layers.is_empty());
                    mem::swap(&mut x.1.layers, &mut layers);
                    false
                }
            });
        }
        for digest in layers {
            self.cache.decrement_refcount(digest);
        }
    }

    fn receive_job_result(&mut self, id: JobId, result: JobResult) {
        // If there is no entry in the executing map, then the job has been canceled
        // and we don't need to send any message to the broker.
        if let Some((details, _)) = self.executing.remove(&id) {
            self.deps
                .send_response_to_broker(WorkerToBroker(id, result));
            for digest in details.layers {
                self.cache.decrement_refcount(digest);
            }
            self.possibly_start_job();
        }
    }

    fn receive_artifact_failure(&mut self, digest: Sha256Digest) {
        for jid in self.cache.got_artifact_failure(digest.clone()) {
            // If this was the first layer error for this request, then we'll find something in the
            // hash table, and we'll need to clean up.
            //
            // Otherwise, it means that there were previous errors for this entry, and there's
            // nothing to do here.
            if let Some(entry) = self.awaiting_layers.remove(&jid) {
                self.deps.send_response_to_broker(WorkerToBroker(
                    jid,
                    JobResult::Error(format!(
                        "Failed to download and extract layer artifact {}",
                        digest
                    )),
                ));
                for digest in entry.layers.into_keys() {
                    self.cache.decrement_refcount(digest);
                }
            }
        }
    }

    fn receive_artifact_success(&mut self, digest: Sha256Digest, bytes_used: u64) {
        for (jid, path) in self.cache.got_artifact_success(digest.clone(), bytes_used) {
            // If there were previous errors for this job, then we'll find
            // nothing in the hash table, and we'll need to release this layer.
            //
            // Otherwise, it means that so far all is good. We then need to check if
            // we've gotten all layers. If we have, then we can go ahead and schedule
            // the job.
            match self.awaiting_layers.entry(jid) {
                hash_map::Entry::Vacant(_) => {
                    self.cache.decrement_refcount(digest.clone());
                }
                hash_map::Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .layers
                        .insert(digest.clone(), path)
                        .assert_is_none();
                    if entry.get().has_all_layers() {
                        let mut entry = entry.remove();
                        let layers = entry
                            .details
                            .layers
                            .iter()
                            .map(|digest| entry.layers.remove(digest).unwrap())
                            .collect();
                        self.queued.push_back((jid, entry.details, layers));
                        self.possibly_start_job();
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
    use itertools::Itertools;
    use meticulous_test::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use BrokerToWorker::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        StartJob(JobId, JobDetails, Vec<PathBuf>),
        DropJobHandle(JobId),
        SendResponseToBroker(WorkerToBroker),
        StartArtifactFetch(Sha256Digest, PathBuf),
        CacheGetArtifact(JobId, Sha256Digest),
        CacheGotArtifactSuccess(Sha256Digest, u64),
        CacheGotArtifactFailure(Sha256Digest),
        CacheDecrementRefcount(Sha256Digest),
    }

    use TestMessage::*;

    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<Sha256Digest, Vec<GetArtifact>>,
        got_artifact_success_returns: HashMap<Sha256Digest, Vec<Vec<(JobId, PathBuf)>>>,
        got_artifact_failure_returns: HashMap<Sha256Digest, Vec<Vec<JobId>>>,
    }

    struct JobHandle {
        id: JobId,
        test_state: Rc<RefCell<TestState>>,
    }

    impl Drop for JobHandle {
        fn drop(&mut self) {
            self.test_state
                .borrow_mut()
                .messages
                .push(DropJobHandle(self.id))
        }
    }

    impl DispatcherDeps for Rc<RefCell<TestState>> {
        type JobHandle = JobHandle;

        fn start_job(
            &mut self,
            id: JobId,
            details: &JobDetails,
            layers: Vec<PathBuf>,
        ) -> JobHandle {
            self.borrow_mut()
                .messages
                .push(StartJob(id, details.clone(), layers));
            JobHandle {
                id,
                test_state: self.clone(),
            }
        }

        fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
            self.borrow_mut()
                .messages
                .push(StartArtifactFetch(digest, path));
        }

        fn send_response_to_broker(&mut self, message: WorkerToBroker) {
            self.borrow_mut()
                .messages
                .push(SendResponseToBroker(message));
        }
    }

    impl DispatcherCache for Rc<RefCell<TestState>> {
        fn get_artifact(&mut self, id: JobId, digest: Sha256Digest) -> GetArtifact {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifact(id, digest.clone()));
            self.borrow_mut()
                .get_artifact_returns
                .get_mut(&digest)
                .unwrap()
                .remove(0)
        }

        fn got_artifact_failure(&mut self, digest: Sha256Digest) -> Vec<JobId> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactFailure(digest.clone()));
            self.borrow_mut()
                .got_artifact_failure_returns
                .get_mut(&digest)
                .unwrap()
                .remove(0)
        }

        fn got_artifact_success(
            &mut self,
            digest: Sha256Digest,
            bytes_used: u64,
        ) -> Vec<(JobId, PathBuf)> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifactSuccess(digest.clone(), bytes_used));
            self.borrow_mut()
                .got_artifact_success_returns
                .get_mut(&digest)
                .unwrap()
                .remove(0)
        }

        fn decrement_refcount(&mut self, digest: Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefcount(digest))
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        dispatcher: Dispatcher<Rc<RefCell<TestState>>, Rc<RefCell<TestState>>>,
    }

    impl Fixture {
        fn new<const L: usize, const M: usize, const N: usize>(
            slots: usize,
            get_artifact_returns: [(Sha256Digest, Vec<GetArtifact>); L],
            got_artifact_success_returns: [(Sha256Digest, Vec<Vec<(JobId, PathBuf)>>); M],
            got_artifact_failure_returns: [(Sha256Digest, Vec<Vec<JobId>>); N],
        ) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
                get_artifact_returns: HashMap::from(get_artifact_returns),
                got_artifact_success_returns: HashMap::from(got_artifact_success_returns),
                got_artifact_failure_returns: HashMap::from(got_artifact_failure_returns),
            }));
            let dispatcher = Dispatcher::new(test_state.clone(), test_state.clone(), slots);
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
        Fixture::new(2, [], [], []),
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
    }

    script_test! {
        enqueue_2,
        Fixture::new(2, [], [], []),
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
    }

    script_test! {
        enqueue_3_with_2_slots,
        Fixture::new(2, [], [], []),
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueJob(jid![3], details![3])) => {};
    }

    script_test! {
        enqueue_1_with_layers_all_available,
        Fixture::new(2, [
            (digest!(41), vec![GetArtifact::Success(path_buf!("/a"))]),
            (digest!(42), vec![GetArtifact::Success(path_buf!("/b"))]),
            (digest!(43), vec![GetArtifact::Success(path_buf!("/c"))])
        ], [], []),
        Broker(EnqueueJob(jid![1], details![1, [41, 42, 43]])) => {
            CacheGetArtifact(jid![1], digest!(41)),
            CacheGetArtifact(jid![1], digest!(42)),
            CacheGetArtifact(jid![1], digest!(43)),
            StartJob(jid![1], details![1, [41, 42, 43]], path_buf_vec!["/a", "/b", "/c"])
        };
    }

    script_test! {
        enqueue_1_with_layers_some_available,
        Fixture::new(
            2, [
                (digest!(41), vec![GetArtifact::Success(path_buf!("/a"))]),
                (digest!(42), vec![GetArtifact::Wait]),
                (digest!(43), vec![GetArtifact::Get(path_buf!("/c"))])
            ], [
                (digest!(42), vec![vec![(jid![1], path_buf!("/b"))]]),
                (digest!(43), vec![vec![(jid![1], path_buf!("/c"))]]),
            ], []),
        Broker(EnqueueJob(jid![1], details![1, [41, 42, 43]])) => {
            CacheGetArtifact(jid![1], digest!(41)),
            CacheGetArtifact(jid![1], digest!(42)),
            CacheGetArtifact(jid![1], digest!(43)),
            StartArtifactFetch(digest!(43), path_buf!("/c")),
        };

        ArtifactFetcher(digest!(42), Some(100)) => {
            CacheGotArtifactSuccess(digest![42], 100),
        };
        ArtifactFetcher(digest!(43), Some(100)) => {
            CacheGotArtifactSuccess(digest![43], 100),
            StartJob(jid![1], details![1, [41, 42, 43]], path_buf_vec!["/a", "/b", "/c"])
        }
    }

    script_test! {
        complete_1,
        Fixture::new(
            2,
            [(digest!(42), vec![GetArtifact::Success(path_buf!("/a"))])],
            [],
            []),
        Broker(EnqueueJob(jid![1], details![1, [42]])) =>  {
            CacheGetArtifact(jid![1], digest!(42)),
            StartJob(jid![1], details![1, [42]], path_buf_vec!["/a"]) };
        Executor(jid![1], result![1]) => {
            DropJobHandle(jid![1]),
            SendResponseToBroker(WorkerToBroker(jid![1], result![1])),
            CacheDecrementRefcount(digest!(42)),
        };
    }

    script_test! {
        complete_1_while_blocked,
        Fixture::new(2, [], [], []),
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueJob(jid![3], details![3])) => {};
        Executor(jid![1], result![1]) => {
            DropJobHandle(jid![1]),
            SendResponseToBroker(WorkerToBroker(jid![1], result![1])),
            StartJob(jid![3], details![3], path_buf_vec![]),
        };
    }

    script_test! {
        enqueue_2_complete_1_enqueue_1,
        Fixture::new(2, [], [], []),
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
        Executor(jid![1], result![1]) => {
            DropJobHandle(jid![1]),
            SendResponseToBroker(WorkerToBroker(jid![1], result![1]))
        };
        Broker(EnqueueJob(jid![3], details![3])) => { StartJob(jid![3], details![3], path_buf_vec![]) };
    }

    script_test! {
        cancel_queued,
        Fixture::new(2, [
            (digest!(41), vec![GetArtifact::Success(path_buf!("/a"))]),
            (digest!(42), vec![GetArtifact::Success(path_buf!("/b"))]),
            (digest!(43), vec![GetArtifact::Success(path_buf!("/c"))]),
        ], [], []),
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueJob(jid![3], details![3, [41, 42, 43]])) => {
            CacheGetArtifact(jid![3], digest!(41)),
            CacheGetArtifact(jid![3], digest!(42)),
            CacheGetArtifact(jid![3], digest!(43)),
        };
        Broker(EnqueueJob(jid![4], details![4])) => {};
        Broker(CancelJob(jid![3])) => {
            CacheDecrementRefcount(digest!(41)),
            CacheDecrementRefcount(digest!(42)),
            CacheDecrementRefcount(digest!(43)),
        };
        Executor(jid![1], result![1]) => {
            DropJobHandle(jid![1]),
            SendResponseToBroker(WorkerToBroker(jid![1], result![1])),
            StartJob(jid![4], details![4], path_buf_vec![]),
        };
    }

    script_test! {
        cancel_executing,
        Fixture::new(2, [
            (digest!(41), vec![GetArtifact::Success(path_buf!("/a"))]),
            (digest!(42), vec![GetArtifact::Success(path_buf!("/b"))]),
            (digest!(43), vec![GetArtifact::Success(path_buf!("/c"))]),
        ], [], []),
        Broker(EnqueueJob(jid![1], details![1, [41, 42, 43]])) => {
            CacheGetArtifact(jid![1], digest!(41)),
            CacheGetArtifact(jid![1], digest!(42)),
            CacheGetArtifact(jid![1], digest!(43)),
            StartJob(jid![1], details![1, [41, 42, 43]], path_buf_vec!["/a", "/b", "/c"])
        };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueJob(jid![3], details![3])) => {};
        Broker(CancelJob(jid![1])) => {
            DropJobHandle(jid![1]),
            CacheDecrementRefcount(digest!(41)),
            CacheDecrementRefcount(digest!(42)),
            CacheDecrementRefcount(digest!(43)),
            StartJob(jid![3], details![3], path_buf_vec![])
        };
        Executor(jid![1], result![2]) => {};
    }

    script_test! {
        cancel_awaiting_layers,
        Fixture::new(2, [
            (digest!(41), vec![GetArtifact::Success(path_buf!("/a"))]),
            (digest!(42), vec![GetArtifact::Wait]),
            (digest!(43), vec![GetArtifact::Wait]),
        ], [], []),
        Broker(EnqueueJob(jid![1], details![1, [41, 42, 43]])) => {
            CacheGetArtifact(jid![1], digest!(41)),
            CacheGetArtifact(jid![1], digest!(42)),
            CacheGetArtifact(jid![1], digest!(43)),
        };
        Broker(CancelJob(jid![1])) => {
            CacheDecrementRefcount(digest!(41))
        }
    }

    script_test! {
        cancels_idempotent,
        Fixture::new(2, [], [], []),
        Broker(CancelJob(jid![2])) => {};
        Broker(CancelJob(jid![2])) => {};
        Broker(EnqueueJob(jid![1], details![1])) => { StartJob(jid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueJob(jid![2], details![2])) => { StartJob(jid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueJob(jid![3], details![3])) => {};
        Broker(EnqueueJob(jid![4], details![4])) => {};
        Broker(CancelJob(jid![4])) => {};
        Broker(CancelJob(jid![4])) => {};
        Broker(CancelJob(jid![4])) => {};
        Broker(CancelJob(jid![2])) => {
            DropJobHandle(jid![2]),
            StartJob(jid![3], details![3], path_buf_vec![]),
        };
        Broker(CancelJob(jid![2])) => {};
        Broker(CancelJob(jid![2])) => {};
        Executor(jid![2], result![2]) => {};
        Broker(CancelJob(jid![2])) => {};
        Broker(CancelJob(jid![2])) => {};
    }

    script_test! {
            error_cache_responses,
            Fixture::new(2, [
                (digest!(41), vec![GetArtifact::Wait]),
                (digest!(42), vec![GetArtifact::Wait]),
                (digest!(43), vec![GetArtifact::Wait]),
                (digest!(44), vec![GetArtifact::Wait]),
            ], [
                (digest!(41), vec![vec![(jid![1], path_buf!("/a"))]]),
                (digest!(43), vec![vec![(jid![1], path_buf!("/c"))]]),
            ], [
                (digest!(42), vec![vec![jid![1]]]),
                (digest!(44), vec![vec![jid![1]]]),
    ]),
            Broker(EnqueueJob(jid![1], details![1, [41, 42, 43, 44]])) => {
                CacheGetArtifact(jid![1], digest!(41)),
                CacheGetArtifact(jid![1], digest!(42)),
                CacheGetArtifact(jid![1], digest!(43)),
                CacheGetArtifact(jid![1], digest!(44)),
            };
            ArtifactFetcher(digest!(41), Some(101)) => {
                CacheGotArtifactSuccess(digest!(41), 101),
            };
            ArtifactFetcher(digest!(42), None) => {
                CacheGotArtifactFailure(digest!(42)),
                SendResponseToBroker(WorkerToBroker(jid![1], JobResult::Error(
                    "Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a".to_string()))),
                CacheDecrementRefcount(digest!(41))
            };
            ArtifactFetcher(digest!(43), Some(103)) => {
                CacheGotArtifactSuccess(digest!(43), 103),
                CacheDecrementRefcount(digest!(43))
            };
            ArtifactFetcher(digest!(44), None) => {
                CacheGotArtifactFailure(digest!(44)),
            };
        }

    #[test]
    #[should_panic(expected = "assertion failed: slots > 0")]
    fn slots_must_be_nonzero() {
        let _ = Fixture::new(0, [], [], []);
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.executing.insert")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(2, [], [], []);
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid![1], details![1])));
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueJob(jid![1], details![2])));
    }
}
