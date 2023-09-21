//! Central processing module for the worker. Receive messages from the broker and executors, and
//! start or cancel executions as appropriate.

use meticulous_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    ExecutionDetails, ExecutionId, ExecutionResult, Sha256Digest,
};
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

/// Manage executions based on the slot count and requests from the broker. If the broker
/// sends more execution requests than there are slots, the extra requests are queued in a FIFO
/// queue. It's up to the broker to order the requests properly.
///
/// All methods are completely nonblocking. They will never block the task or the thread.
pub struct Dispatcher<D: DispatcherDeps> {
    deps: D,
    slots: usize,
    awaiting_layers: HashMap<ExecutionId, AwaitingLayersEntry>,
    queued: VecDeque<(ExecutionId, ExecutionDetails, Vec<PathBuf>)>,
    executing: HashMap<ExecutionId, (ExecutionDetails, D::ExecutionHandle)>,
}

/// The external dependencies for [Dispatcher]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait DispatcherDeps {
    /// A handle to a pending execution. When dropped, the handle must tell the execution to
    /// terminate immediately. However, the drop method must not wait for the execution to
    /// terminate. Instead, that must happen asynchronously, and the actual execution termination
    /// notification must come through as a [Message::Executor] message.
    ///
    /// It must be safe to drop the handle after the execution has terminated.
    type ExecutionHandle;

    /// Start a new execution. When the execution terminates, the notification must come through as
    /// a [Message::Executor] message.
    fn start_execution(
        &mut self,
        id: ExecutionId,
        details: &ExecutionDetails,
        layers: Vec<PathBuf>,
    ) -> Self::ExecutionHandle;

    /// Send a message to the broker.
    fn send_response_to_broker(&mut self, message: WorkerToBroker);

    /// Send a [super::cache::Message::GetRequest] to the cache.
    fn send_get_request_to_cache(&mut self, id: ExecutionId, digest: Sha256Digest);

    /// Send a [super::cache::Message::DecrementRefcount] to the cache.
    fn send_decrement_refcount_to_cache(&mut self, digest: Sha256Digest);
}

/// An input message for the dispatcher. These come from either the broker or from an executor.
#[derive(Debug)]
pub enum Message {
    Broker(BrokerToWorker),
    Executor(ExecutionId, ExecutionResult),
    Cache(ExecutionId, Sha256Digest, Option<PathBuf>),
}

impl<D: DispatcherDeps> Dispatcher<D> {
    /// Create a new dispatcher with the provided slot count. The slot count must be a positive
    /// number.
    pub fn new(deps: D, slots: usize) -> Self {
        assert!(slots > 0);
        Dispatcher {
            deps,
            slots,
            awaiting_layers: HashMap::new(),
            queued: VecDeque::new(),
            executing: HashMap::new(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [Message] for more information.
    pub fn receive_message(&mut self, msg: Message) {
        match msg {
            Message::Broker(BrokerToWorker::EnqueueExecution(id, details)) => {
                self.receive_enqueue_execution(id, details)
            }
            Message::Broker(BrokerToWorker::CancelExecution(id)) => {
                self.receive_cancel_execution(id)
            }
            Message::Executor(id, result) => self.receive_execution_result(id, result),
            Message::Cache(id, digest, path) => self.receive_cache_response(id, digest, path),
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

impl<D: DispatcherDeps> Dispatcher<D> {
    fn possibly_start_execution(&mut self) {
        if self.executing.len() < self.slots {
            if let Some((id, details, layer_paths)) = self.queued.pop_front() {
                let handle = self.deps.start_execution(id, &details, layer_paths);
                assert!(self.executing.insert(id, (details, handle)).is_none());
            }
        }
    }

    fn receive_enqueue_execution(&mut self, id: ExecutionId, details: ExecutionDetails) {
        if details.layers.is_empty() {
            self.queued.push_back((id, details, vec![]));
            self.possibly_start_execution();
        } else {
            for digest in &details.layers {
                self.deps.send_get_request_to_cache(id, digest.clone());
            }
            assert!(self
                .awaiting_layers
                .insert(id, AwaitingLayersEntry::new(details))
                .is_none());
        }
    }

    fn receive_cancel_execution(&mut self, id: ExecutionId) {
        let mut layers = vec![];
        if let Some(entry) = self.awaiting_layers.remove(&id) {
            // We may have already gotten some layers. Make sure we release those.
            layers = entry.layers.into_keys().collect();
        } else if let Some((details, _)) = self.executing.remove(&id) {
            // If it was executing, we need to drop the execution handle, which will
            // tell the executor to kill the process. We also need to clean up all of our layers.
            layers = details.layers;
            self.possibly_start_execution();
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
            self.deps.send_decrement_refcount_to_cache(digest);
        }
    }

    fn receive_execution_result(&mut self, id: ExecutionId, result: ExecutionResult) {
        // If there is no entry in the executing map, then the execution has been canceled
        // and we don't need to send any message to the broker.
        if let Some((details, _)) = self.executing.remove(&id) {
            self.deps
                .send_response_to_broker(WorkerToBroker(id, result));
            for digest in details.layers {
                self.deps.send_decrement_refcount_to_cache(digest);
            }
            self.possibly_start_execution();
        }
    }

    fn receive_cache_response(
        &mut self,
        id: ExecutionId,
        digest: Sha256Digest,
        path: Option<PathBuf>,
    ) {
        match path {
            None => {
                // There was an error getting the layer.
                //
                // If this was the first layer error for this request, then we'll find
                // something in the hash table, and we'll need to clean up.
                //
                // Otherwise, it means that there were previous errors for this entry, and
                // there's nothing to do here.
                if let Some(entry) = self.awaiting_layers.remove(&id) {
                    self.deps.send_response_to_broker(WorkerToBroker(
                        id,
                        ExecutionResult::Error(format!(
                            "Failed to download and extract layer artifact {}",
                            digest
                        )),
                    ));
                    for digest in entry.layers.into_keys() {
                        self.deps.send_decrement_refcount_to_cache(digest);
                    }
                }
            }
            Some(path) => {
                // We got one of the layers back successfully.
                //
                // If there were previous errors for this execution, then we'll find
                // nothing in the hash table, and we'll need to release this layer.
                //
                // Otherwise, it means that so far all is good. We then need to check if
                // we've gotten all layers. If we have, then we can go ahead and schedule
                // the execution.
                match self.awaiting_layers.entry(id) {
                    hash_map::Entry::Vacant(_) => {
                        self.deps.send_decrement_refcount_to_cache(digest);
                    }
                    hash_map::Entry::Occupied(mut oe) => {
                        assert!(oe.get_mut().layers.insert(digest, path).is_none());
                        if oe.get().has_all_layers() {
                            let mut entry = oe.remove();
                            let layers = entry
                                .details
                                .layers
                                .iter()
                                .map(|digest| entry.layers.remove(digest).unwrap())
                                .collect();
                            self.queued.push_back((id, entry.details, layers));
                            self.possibly_start_execution();
                        }
                    }
                }
            }
        }
    }
}

struct AwaitingLayersEntry {
    details: ExecutionDetails,
    layers: HashMap<Sha256Digest, PathBuf>,
}

impl AwaitingLayersEntry {
    fn new(details: ExecutionDetails) -> Self {
        AwaitingLayersEntry {
            details,
            layers: HashMap::new(),
        }
    }

    fn has_all_layers(&self) -> bool {
        self.layers.len() == self.details.layers.len()
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
    use meticulous_test::*;
    use itertools::Itertools;
    use std::cell::RefCell;
    use std::rc::Rc;
    use BrokerToWorker::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        StartExecution(ExecutionId, ExecutionDetails, Vec<PathBuf>),
        DropExecutionHandle(ExecutionId),
        SendResponseToBroker(WorkerToBroker),
        SendGetRequestToCache(ExecutionId, Sha256Digest),
        SendDecrementRefcountToCache(Sha256Digest),
    }

    use TestMessage::*;

    struct TestState {
        messages: Vec<TestMessage>,
    }

    impl TestState {
        fn new() -> Self {
            TestState {
                messages: Vec::new(),
            }
        }
    }

    struct ExecutionHandle {
        id: ExecutionId,
        test_state: Rc<RefCell<TestState>>,
    }

    impl Drop for ExecutionHandle {
        fn drop(&mut self) {
            self.test_state
                .borrow_mut()
                .messages
                .push(DropExecutionHandle(self.id))
        }
    }

    impl DispatcherDeps for Rc<RefCell<TestState>> {
        type ExecutionHandle = ExecutionHandle;

        fn start_execution(
            &mut self,
            id: ExecutionId,
            details: &ExecutionDetails,
            layers: Vec<PathBuf>,
        ) -> ExecutionHandle {
            self.borrow_mut()
                .messages
                .push(StartExecution(id, details.clone(), layers));
            ExecutionHandle {
                id,
                test_state: self.clone(),
            }
        }

        fn send_response_to_broker(&mut self, message: WorkerToBroker) {
            self.borrow_mut()
                .messages
                .push(SendResponseToBroker(message));
        }

        fn send_get_request_to_cache(&mut self, id: ExecutionId, digest: Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(SendGetRequestToCache(id, digest))
        }

        fn send_decrement_refcount_to_cache(&mut self, digest: Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(SendDecrementRefcountToCache(digest))
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        dispatcher: Dispatcher<Rc<RefCell<TestState>>>,
    }

    impl Fixture {
        fn new(slots: usize) -> Self {
            let test_state = Rc::new(RefCell::new(TestState::new()));
            let dispatcher = Dispatcher::new(test_state.clone(), slots);
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
        ($test_name:ident, $slots:expr, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = Fixture::new($slots);
                $(
                    fixture.dispatcher.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )+
            }
        };
    }

    script_test! {
        enqueue_1,
        2,
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
    }

    script_test! {
        enqueue_2,
        2,
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
    }

    script_test! {
        enqueue_3_with_2_slots,
        2,
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![3], details![3])) => {};
    }

    script_test! {
        enqueue_1_with_layers,
        2,
        Broker(EnqueueExecution(eid![1], details![1, [41, 42, 43]])) => {
            SendGetRequestToCache(eid![1], digest!(41)),
            SendGetRequestToCache(eid![1], digest!(42)),
            SendGetRequestToCache(eid![1], digest!(43)),
        };

        Cache(eid![1], digest!(43), Some(path_buf!("/c"))) => {};
        Cache(eid![1], digest!(41), Some(path_buf!("/a"))) => {};
        Cache(eid![1], digest!(42), Some(path_buf!("/b"))) => {
            StartExecution(eid![1], details![1, [41, 42, 43]], path_buf_vec!["/a", "/b", "/c"])
        };
    }

    script_test! {
        complete_1,
        2,
        Broker(EnqueueExecution(eid![1], details![1, [42]])) =>  {
            SendGetRequestToCache(eid![1], digest!(42)),
        };
        Cache(eid![1], digest!(42), Some(path_buf!("/a"))) => {StartExecution(eid![1], details![1, [42]], path_buf_vec!["/a"]) };
        Executor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerToBroker(eid![1], result![1])),
            SendDecrementRefcountToCache(digest!(42)),
        };
    }

    script_test! {
        complete_1_while_blocked,
        2,
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![3], details![3])) => {};
        Executor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerToBroker(eid![1], result![1])),
            StartExecution(eid![3], details![3], path_buf_vec![]),
        };
    }

    script_test! {
        enqueue_2_complete_1_enqueue_1,
        2,
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
        Executor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerToBroker(eid![1], result![1]))
        };
        Broker(EnqueueExecution(eid![3], details![3])) => { StartExecution(eid![3], details![3], path_buf_vec![]) };
    }

    script_test! {
        cancel_queued,
        2,
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![3], details![3, [41, 42, 43]])) => {
            SendGetRequestToCache(eid![3], digest!(41)),
            SendGetRequestToCache(eid![3], digest!(42)),
            SendGetRequestToCache(eid![3], digest!(43)),
        };
        Cache(eid![3], digest!(41), Some(path_buf!("/a"))) => {};
        Cache(eid![3], digest!(42), Some(path_buf!("/b"))) => {};
        Cache(eid![3], digest!(43), Some(path_buf!("/c"))) => {};
        Broker(EnqueueExecution(eid![4], details![4])) => {};
        Broker(CancelExecution(eid![3])) => {
            SendDecrementRefcountToCache(digest!(41)),
            SendDecrementRefcountToCache(digest!(42)),
            SendDecrementRefcountToCache(digest!(43)),
        };
        Executor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerToBroker(eid![1], result![1])),
            StartExecution(eid![4], details![4], path_buf_vec![]),
        };
    }

    script_test! {
        cancel_executing,
        2,
        Broker(EnqueueExecution(eid![1], details![1, [41, 42, 43]])) => {
            SendGetRequestToCache(eid![1], digest!(41)),
            SendGetRequestToCache(eid![1], digest!(42)),
            SendGetRequestToCache(eid![1], digest!(43)),
        };
        Cache(eid![1], digest!(41), Some(path_buf!("/a"))) => {};
        Cache(eid![1], digest!(42), Some(path_buf!("/b"))) => {};
        Cache(eid![1], digest!(43), Some(path_buf!("/c"))) => {
            StartExecution(eid![1], details![1, [41, 42, 43]], path_buf_vec!["/a", "/b", "/c"])
        };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![3], details![3])) => {};
        Broker(CancelExecution(eid![1])) => {
            DropExecutionHandle(eid![1]),
            SendDecrementRefcountToCache(digest!(41)),
            SendDecrementRefcountToCache(digest!(42)),
            SendDecrementRefcountToCache(digest!(43)),
            StartExecution(eid![3], details![3], path_buf_vec![])
        };
        Executor(eid![1], result![2]) => {};
    }

    script_test! {
        cancel_awaiting_layers,
        2,
        Broker(EnqueueExecution(eid![1], details![1, [41, 42, 43]])) => {
            SendGetRequestToCache(eid![1], digest!(41)),
            SendGetRequestToCache(eid![1], digest!(42)),
            SendGetRequestToCache(eid![1], digest!(43)),
        };
        Cache(eid![1], digest!(41), Some(path_buf!("/a"))) => {};
        Broker(CancelExecution(eid![1])) => {
            SendDecrementRefcountToCache(digest!(41))
        }
    }

    script_test! {
        cancels_idempotent,
        2,
        Broker(CancelExecution(eid![2])) => {};
        Broker(CancelExecution(eid![2])) => {};
        Broker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2], path_buf_vec![]) };
        Broker(EnqueueExecution(eid![3], details![3])) => {};
        Broker(EnqueueExecution(eid![4], details![4])) => {};
        Broker(CancelExecution(eid![4])) => {};
        Broker(CancelExecution(eid![4])) => {};
        Broker(CancelExecution(eid![4])) => {};
        Broker(CancelExecution(eid![2])) => {
            DropExecutionHandle(eid![2]),
            StartExecution(eid![3], details![3], path_buf_vec![]),
        };
        Broker(CancelExecution(eid![2])) => {};
        Broker(CancelExecution(eid![2])) => {};
        Executor(eid![2], result![2]) => {};
        Broker(CancelExecution(eid![2])) => {};
        Broker(CancelExecution(eid![2])) => {};
    }

    script_test! {
        error_cache_responses,
        2,
        Broker(EnqueueExecution(eid![1], details![1, [41, 42, 43, 44]])) => {
            SendGetRequestToCache(eid![1], digest!(41)),
            SendGetRequestToCache(eid![1], digest!(42)),
            SendGetRequestToCache(eid![1], digest!(43)),
            SendGetRequestToCache(eid![1], digest!(44)),
        };
        Cache(eid![1], digest!(41), Some(path_buf!("/a"))) => {};
        Cache(eid![1], digest!(42), None) => {
            SendResponseToBroker(WorkerToBroker(eid![1], ExecutionResult::Error(
                "Failed to download and extract layer artifact 000000000000000000000000000000000000000000000000000000000000002a".to_string()))),
            SendDecrementRefcountToCache(digest!(41))
        };
        Cache(eid![1], digest!(43), Some(path_buf!("/c"))) => {
            SendDecrementRefcountToCache(digest!(43))
        };
        Cache(eid![1], digest!(44), None) => {};
    }

    #[test]
    #[should_panic(expected = "assertion failed: slots > 0")]
    fn slots_must_be_nonzero() {
        let _ = Fixture::new(0);
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.executing.insert")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(2);
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueExecution(eid![1], details![1])));
        fixture
            .dispatcher
            .receive_message(Broker(EnqueueExecution(eid![1], details![2])));
    }
}
