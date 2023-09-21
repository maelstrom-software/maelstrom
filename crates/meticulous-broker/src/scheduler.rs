//! Central processing module for the broker. Receives and sends messages to and from clients and
//! workers.

use meticulous_util::heap::{Heap, HeapDeps, HeapIndex};
use meticulous::{
    proto::{BrokerToClient, BrokerToWorker, ClientToBroker, WorkerToBroker},
    ClientExecutionId, ClientId, ExecutionDetails, ExecutionId, ExecutionResult, WorkerId,
};
use std::collections::{HashMap, VecDeque};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// All methods are completely nonblocking. They will never block the task or the thread.
pub struct Scheduler<DepsT: SchedulerDeps> {
    clients: HashMap<ClientId, DepsT::ClientSender>,
    workers: WorkerMap<DepsT>,
    queued_requests: VecDeque<(ExecutionId, ExecutionDetails)>,
    worker_heap: Heap<WorkerMap<DepsT>>,
}

/// The external dependencies for [Scheduler]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait SchedulerDeps {
    type ClientSender;
    type WorkerSender;
    fn send_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        response: BrokerToClient,
    );
    fn send_request_to_worker(&mut self, sender: &mut Self::WorkerSender, request: BrokerToWorker);
}

#[derive(Debug)]
pub enum Message<DepsT: SchedulerDeps> {
    ClientConnected(ClientId, DepsT::ClientSender),
    ClientDisconnected(ClientId),
    FromClient(ClientId, ClientToBroker),
    WorkerConnected(WorkerId, usize, DepsT::WorkerSender),
    WorkerDisconnected(WorkerId),
    FromWorker(WorkerId, WorkerToBroker),
}

impl<DepsT: SchedulerDeps> Scheduler<DepsT> {
    pub fn receive_message(&mut self, deps: &mut DepsT, msg: Message<DepsT>) {
        match msg {
            Message::ClientConnected(id, sender) => self.receive_client_connected(id, sender),
            Message::ClientDisconnected(id) => self.receive_client_disconnected(deps, id),
            Message::FromClient(cid, ClientToBroker::ExecutionRequest(ceid, details)) => {
                self.receive_client_request(deps, cid, ceid, details)
            }
            Message::FromClient(cid, ClientToBroker::UiRequest(msg)) => {
                self.receive_ui_request(deps, cid, msg)
            }
            Message::WorkerConnected(id, slots, sender) => {
                self.receive_worker_connected(deps, id, slots, sender)
            }
            Message::WorkerDisconnected(id) => self.receive_worker_disconnected(deps, id),
            Message::FromWorker(wid, WorkerToBroker(eid, result)) => {
                self.receive_worker_response(deps, wid, eid, result)
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

#[derive(Debug)]
struct Worker<DepsT: SchedulerDeps> {
    slots: usize,
    pending: HashMap<ExecutionId, ExecutionDetails>,
    heap_index: HeapIndex,
    sender: DepsT::WorkerSender,
}

impl<DepsT: SchedulerDeps> Default for Scheduler<DepsT> {
    fn default() -> Self {
        Scheduler {
            clients: HashMap::default(),
            workers: WorkerMap(HashMap::default()),
            queued_requests: VecDeque::default(),
            worker_heap: Heap::default(),
        }
    }
}

struct WorkerMap<DepsT: SchedulerDeps>(HashMap<WorkerId, Worker<DepsT>>);

impl<DepsT: SchedulerDeps> HeapDeps for WorkerMap<DepsT> {
    type Element = WorkerId;

    fn is_element_less_than(&self, lhs_id: &WorkerId, rhs_id: &WorkerId) -> bool {
        let lhs_worker = self.0.get(lhs_id).unwrap();
        let rhs_worker = self.0.get(rhs_id).unwrap();
        let lhs = (lhs_worker.pending.len() * rhs_worker.slots, *lhs_id);
        let rhs = (rhs_worker.pending.len() * lhs_worker.slots, *rhs_id);
        lhs.cmp(&rhs) == std::cmp::Ordering::Less
    }

    fn update_index(&mut self, elem: &WorkerId, idx: HeapIndex) {
        self.0.get_mut(elem).unwrap().heap_index = idx;
    }
}

impl<DepsT: SchedulerDeps> Scheduler<DepsT> {
    fn possibly_start_executions(&mut self, deps: &mut DepsT) {
        while !self.queued_requests.is_empty() && !self.workers.0.is_empty() {
            let wid = self.worker_heap.peek().unwrap();
            let worker = self.workers.0.get_mut(wid).unwrap();

            if worker.pending.len() == 2 * worker.slots {
                break;
            }

            let (eid, details) = self.queued_requests.pop_front().unwrap();
            deps.send_request_to_worker(
                &mut worker.sender,
                BrokerToWorker::EnqueueExecution(eid, details.clone()),
            );

            worker.pending.insert(eid, details);
            let heap_index = worker.heap_index;
            self.worker_heap.sift_down(&mut self.workers, heap_index);
        }
    }

    fn receive_client_connected(&mut self, id: ClientId, sender: DepsT::ClientSender) {
        assert!(
            self.clients.insert(id, sender).is_none(),
            "duplicate client id {id:?}"
        );
    }

    fn receive_client_disconnected(&mut self, deps: &mut DepsT, id: ClientId) {
        assert!(self.clients.remove(&id).is_some());
        self.queued_requests
            .retain(|(ExecutionId(cid, _), _)| *cid != id);
        for worker in self.workers.0.values_mut() {
            worker.pending.retain(|eid, _| {
                eid.0 != id || {
                    deps.send_request_to_worker(
                        &mut worker.sender,
                        BrokerToWorker::CancelExecution(*eid),
                    );
                    false
                }
            });
        }
        self.worker_heap.rebuild(&mut self.workers);
        self.possibly_start_executions(deps);
    }

    fn receive_client_request(
        &mut self,
        deps: &mut DepsT,
        cid: ClientId,
        ceid: ClientExecutionId,
        details: ExecutionDetails,
    ) {
        assert!(self.clients.contains_key(&cid), "unknown client id {cid:?}");
        self.queued_requests
            .push_back((ExecutionId(cid, ceid), details));
        self.possibly_start_executions(deps);
    }

    fn receive_ui_request(
        &mut self,
        deps: &mut DepsT,
        client_id: ClientId,
        request: meticulous_ui::Request,
    ) {
        use meticulous_ui::{BrokerStatistics, Request, Response};

        let resp = match request {
            Request::GetStatistics => Response::GetStatistics(BrokerStatistics {
                num_clients: self.clients.len() as u64,
                num_workers: self.workers.0.len() as u64,
                num_requests: self.queued_requests.len() as u64,
            }),
        };
        deps.send_response_to_client(
            self.clients.get_mut(&client_id).unwrap(),
            BrokerToClient::UiResponse(resp),
        );
    }

    fn receive_worker_connected(
        &mut self,
        deps: &mut DepsT,
        id: WorkerId,
        slots: usize,
        sender: DepsT::WorkerSender,
    ) {
        let unique = self
            .workers
            .0
            .insert(
                id,
                Worker {
                    slots,
                    pending: HashMap::default(),
                    heap_index: HeapIndex::default(),
                    sender,
                },
            )
            .is_none();
        assert!(unique, "duplicate worker id {id:?}");
        self.worker_heap.push(&mut self.workers, id);
        self.possibly_start_executions(deps);
    }

    fn receive_worker_disconnected(&mut self, deps: &mut DepsT, id: WorkerId) {
        let mut worker = self.workers.0.remove(&id).unwrap();
        self.worker_heap
            .remove(&mut self.workers, worker.heap_index);

        // We sort the requests to keep our tests deterministic.
        let mut vec: Vec<_> = worker.pending.drain().collect();
        vec.sort_by_key(|x| x.0);
        for x in vec.into_iter().rev() {
            self.queued_requests.push_front(x);
        }

        self.possibly_start_executions(deps);
    }

    fn receive_worker_response(
        &mut self,
        deps: &mut DepsT,
        wid: WorkerId,
        eid: ExecutionId,
        result: ExecutionResult,
    ) {
        let worker = self.workers.0.get_mut(&wid).unwrap();

        if worker.pending.remove(&eid).is_none() {
            // This indicates that the client isn't around anymore. Just ignore this response from
            // the worker. When the client disconnected, we canceled all of the outstanding
            // requests and updated our version of the worker's pending requests.
            return;
        }

        deps.send_response_to_client(
            self.clients.get_mut(&eid.0).unwrap(),
            BrokerToClient::ExecutionResponse(eid.1, result),
        );

        if let Some((eid, details)) = self.queued_requests.pop_front() {
            // If there are any queued_requests, we can just pop one off of the front of
            // the queue and not have to update the worker's used slot count or position in the
            // workers list.
            deps.send_request_to_worker(
                &mut worker.sender,
                BrokerToWorker::EnqueueExecution(eid, details.clone()),
            );
            worker.pending.insert(eid, details);
        } else {
            // Since there are no queued_requests, we're going to have to update the
            // worker's position in the workers list.
            let heap_index = worker.heap_index;
            self.worker_heap.sift_up(&mut self.workers, heap_index);
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
    use meticulous::{
        proto::BrokerToWorker::{self, *},
        *
    };
    use itertools::Itertools;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        ToClient(ClientId, BrokerToClient),
        ToWorker(WorkerId, BrokerToWorker),
    }

    use TestMessage::*;

    struct TestClientSender(ClientId);
    struct TestWorkerSender(WorkerId);

    #[derive(Default)]
    struct TestState {
        messages: Vec<TestMessage>,
    }

    impl SchedulerDeps for TestState {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;

        fn send_response_to_client(
            &mut self,
            sender: &mut TestClientSender,
            response: BrokerToClient,
        ) {
            self.messages.push(ToClient(sender.0, response));
        }

        fn send_request_to_worker(
            &mut self,
            sender: &mut TestWorkerSender,
            request: BrokerToWorker,
        ) {
            self.messages.push(ToWorker(sender.0, request));
        }
    }

    #[derive(Default)]
    struct Fixture {
        test_state: TestState,
        scheduler: Scheduler<TestState>,
    }

    impl Fixture {
        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let messages = &mut self.test_state.messages;
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

        fn receive_message(&mut self, msg: Message<TestState>) {
            self.scheduler.receive_message(&mut self.test_state, msg);
        }
    }

    macro_rules! client_sender {
        [$n:expr] => { TestClientSender(cid![$n]) };
    }

    macro_rules! worker_sender {
        [$n:expr] => { TestWorkerSender(wid![$n]) };
    }

    macro_rules! script_test {
        ($test_name:ident, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = Fixture::default();
                $(
                    fixture.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )+
            }
        };
    }

    script_test! {
        message_from_known_client_ok,
        ClientConnected(cid![1], client_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {};
    }

    #[test]
    #[should_panic]
    fn request_from_unknown_client_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(FromClient(
            cid![1],
            ClientToBroker::ExecutionRequest(ceid![1], details![1]),
        ));
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_client_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(ClientDisconnected(cid![1]));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_client_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(ClientDisconnected(cid![1]));
        fixture.receive_message(ClientDisconnected(cid![1]));
    }

    #[test]
    #[should_panic]
    fn response_from_unknown_worker_panics() {
        let mut fixture = Fixture::default();
        // The response will be ignored unless we use a valid ClientId.
        fixture.receive_message(ClientConnected(cid![1], client_sender![1]));

        fixture.receive_message(FromWorker(wid![1], WorkerToBroker(eid![1], result![1])));
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_worker_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(WorkerDisconnected(wid![1]));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_worker_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(WorkerConnected(wid![1], 2, worker_sender![1]));
        fixture.receive_message(WorkerConnected(wid![1], 2, worker_sender![1]));
    }

    script_test! {
        response_from_known_worker_for_unknown_execution_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromWorker(wid![1], WorkerToBroker(eid![1], result![1])) => {};
    }

    script_test! {
        one_client_one_worker,
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1], details![1])),
        };
        FromWorker(wid![1], WorkerToBroker(eid![1], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![1], result![1])),
        };
    }

    script_test! {
        response_from_worker_for_disconnected_client_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromWorker(wid![1], WorkerToBroker(eid![1], result![1])) => {};
    }

    script_test! {
        requests_go_to_workers_based_on_subscription_percentage,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        WorkerConnected(wid![2], 2, worker_sender![2]) => {};
        WorkerConnected(wid![3], 3, worker_sender![3]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        // 0/2 0/2 0/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        // 1/2 0/2 0/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
        };

        // 1/2 1/2 0/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {
            ToWorker(wid![3], EnqueueExecution(eid![1, 3], details![3])),
        };

        // 1/2 1/2 1/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {
            ToWorker(wid![3], EnqueueExecution(eid![1, 4], details![4])),
        };

        // 1/2 1/2 2/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![5], details![5])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 5], details![5])),
        };

        // 2/2 1/2 2/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![6], details![6])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 6], details![6])),
        };

        // 2/2 2/2 2/3
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![7], details![7])) => {
            ToWorker(wid![3], EnqueueExecution(eid![1, 7], details![7])),
        };

        FromWorker(wid![1], WorkerToBroker(eid![1, 1], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![1], result![1])),
        };
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![8], details![8])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 8], details![8])),
        };

        FromWorker(wid![2], WorkerToBroker(eid![1, 2], result![2])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![2], result![2])),
        };
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![9], details![9])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 9], details![9])),
        };

        FromWorker(wid![3], WorkerToBroker(eid![1, 3], result![3])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![3], result![3])),
        };
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![10], details![10])) => {
            ToWorker(wid![3], EnqueueExecution(eid![1, 10], details![10])),
        };
    }

    script_test! {
        requests_start_queueing_at_2x_workers_slot_count,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        // 0/1 0/1
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        // 1/1 0/1
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
        };

        // 1/1 1/1
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 3], details![3])),
        };

        // 2/1 1/1
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 4], details![4])),
        };

        // 2/1 2/1
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![5], details![5])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![6], details![6])) => {};

        // 2/2 1/2
        FromWorker(wid![2], WorkerToBroker(eid![1, 2], result![2])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![2], result![2])),
            ToWorker(wid![2], EnqueueExecution(eid![1, 5], details![5])),
        };

        // 1/2 2/2
        FromWorker(wid![1], WorkerToBroker(eid![1, 1], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![1], result![1])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 6], details![6])),
        };
    }

    script_test! {
        queued_requests_go_to_workers_on_connect,
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![5], details![5])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![6], details![6])) => {};

        WorkerConnected(wid![1], 2, worker_sender![1]) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 2], details![2])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 3], details![3])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 4], details![4])),
        };

        WorkerConnected(wid![2], 2, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 5], details![5])),
            ToWorker(wid![2], EnqueueExecution(eid![1, 6], details![6])),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_get_sent_to_new_workers,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        WorkerConnected(wid![3], 1, worker_sender![3]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {
            ToWorker(wid![3], EnqueueExecution(eid![1, 3], details![3])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 4], details![4])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![5], details![5])) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 5], details![5])),
        };

        WorkerDisconnected(wid![1]) => {
            ToWorker(wid![3], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromWorker(wid![2], WorkerToBroker(eid![1, 2], result![2])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![2], result![2])),
            ToWorker(wid![2], EnqueueExecution(eid![1, 4], details![4])),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_get_sent_to_new_workers_2,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 2], details![2])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {};

        FromWorker(wid![1], WorkerToBroker(eid![1, 1], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![1], result![1])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 3], details![3])),
        };

        WorkerConnected(wid![2], 1, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 4], details![4])),
        };

        WorkerDisconnected(wid![1]) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
        };

        FromWorker(wid![2], WorkerToBroker(eid![1, 2], result![2])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![2], result![2])),
            ToWorker(wid![2], EnqueueExecution(eid![1, 3], details![3])),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_go_to_head_of_queue_for_other_workers,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 2], details![2])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {};

        WorkerDisconnected(wid![1]) => {};

        WorkerConnected(wid![2], 1, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueExecution(eid![1, 1], details![1])),
            ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
        };
    }

    script_test! {
        requests_get_removed_from_workers_pending_map,

        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 2], details![2])),
        };

        FromWorker(wid![1], WorkerToBroker(eid![1, 1], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![1], result![1])),
        };

        FromWorker(wid![1], WorkerToBroker(eid![1, 2], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![2], result![1])),
        };

        WorkerDisconnected(wid![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
    }

    script_test! {
        client_disconnects_with_outstanding_work_1,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelExecution(eid![1, 1])),
        };
    }

    script_test! {
        client_disconnects_with_outstanding_work_2,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};
        ClientConnected(cid![2], client_sender![2]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![2], EnqueueExecution(eid![2, 1], details![1])),
        };

        //ClientDisconnected(cid![2]) => {
        //    ToWorker(wid![2], CancelExecution(eid![2, 1])),
        //};

        //FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
        //    ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
        //};
    }

    script_test! {
        client_disconnects_with_outstanding_work_3,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 2], details![2])),
        };

        ClientConnected(cid![2], client_sender![2]) => {};
        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {};

        ClientDisconnected(cid![2]) => {};

        FromWorker(wid![1], WorkerToBroker(eid![1, 1], result![1])) => {
            ToClient(cid![1], BrokerToClient::ExecutionResponse(ceid![1], result![1])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 3], details![3])),
        };
    }

    script_test! {
        client_disconnects_with_outstanding_work_4,
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};
        ClientConnected(cid![2], client_sender![2]) => {};

        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![1], EnqueueExecution(eid![1, 1], details![1])),
        };

        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![1], details![1])) => {
            ToWorker(wid![2], EnqueueExecution(eid![2, 1], details![1])),
        };

        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {
            ToWorker(wid![1], EnqueueExecution(eid![2, 2], details![2])),
        };

        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {
            ToWorker(wid![2], EnqueueExecution(eid![2, 3], details![3])),
        };

        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![2], details![2])) => {};
        FromClient(cid![2], ClientToBroker::ExecutionRequest(ceid![5], details![5])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![3], details![3])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![4], details![4])) => {};
        FromClient(cid![1], ClientToBroker::ExecutionRequest(ceid![5], details![5])) => {};

        ClientDisconnected(cid![2]) => {
            ToWorker(wid![2], CancelExecution(eid![2, 1])),
            ToWorker(wid![1], CancelExecution(eid![2, 2])),
            ToWorker(wid![2], CancelExecution(eid![2, 3])),

            ToWorker(wid![2], EnqueueExecution(eid![1, 2], details![2])),
            ToWorker(wid![1], EnqueueExecution(eid![1, 3], details![3])),
            ToWorker(wid![2], EnqueueExecution(eid![1, 4], details![4])),
        };
    }
}
