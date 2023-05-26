//! Central processing module for the worker. Receive messages from the broker and executors, and
//! start or cancel executions as appropriate.

use crate::{
    proto::{WorkerRequest, WorkerResponse},
    ExecutionDetails, ExecutionId, ExecutionResult,
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

/// Manage executions based on the slot count and requests from the broker. If the broker
/// sends more execution requests than there are slots, the extra requests are queued in a FIFO
/// queue. It's up to the broker to order the requests properly.
///
/// All methods are completely nonblocking. They will never block the task or the thread.
pub struct Dispatcher<D: DispatcherDeps> {
    deps: D,
    slots: usize,
    blocked: VecDeque<(ExecutionId, ExecutionDetails)>,
    executing: HashMap<ExecutionId, D::ExecutionHandle>,
}

/// The external dependencies for [Dispatcher]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait DispatcherDeps {
    /// A handle to a pending execution. When dropped, the handle must tell the execution to
    /// terminate immediately. However, the drop method must not wait for the execution to
    /// terminate. Instead, that must happen asynchronously, and the actual execution termination
    /// notification must come through as a [Message::FromExecutor] message.
    ///
    /// It must be safe to drop the handle after the execution has terminated.
    type ExecutionHandle;

    /// Start a new execution. When the execution terminates, the notification must come through as
    /// a [Message::FromExecutor] message.
    fn start_execution(
        &mut self,
        id: ExecutionId,
        details: ExecutionDetails,
    ) -> Self::ExecutionHandle;

    /// Send a message to the broker.
    fn send_response_to_broker(&mut self, message: WorkerResponse);
}

/// An input message for the dispatcher. These come from either the broker or from an executor.
#[derive(Debug)]
pub enum Message {
    FromBroker(WorkerRequest),
    FromExecutor(ExecutionId, ExecutionResult),
}

impl<D: DispatcherDeps> Dispatcher<D> {
    /// Create a new dispatcher with the provided slot count. The slot count must be a positive
    /// number.
    pub fn new(deps: D, slots: usize) -> Self {
        assert!(slots > 0);
        Dispatcher {
            deps,
            slots,
            blocked: VecDeque::new(),
            executing: HashMap::new(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [Message] for more information.
    pub fn receive_message(&mut self, msg: Message) {
        match msg {
            Message::FromBroker(WorkerRequest::EnqueueExecution(id, details)) => {
                self.blocked.push_back((id, details));
                self.possibly_start_execution();
            }
            Message::FromBroker(WorkerRequest::CancelExecution(id)) => {
                // Remove execution handle from executing map, which will drop it, which will tell
                // the executor to kill the process.
                if self.executing.remove(&id).is_none() {
                    // If it's not in the executing map, then it may be in the blocked queue.
                    self.blocked.retain(|x| x.0 != id);
                }
            }
            Message::FromExecutor(id, result) => {
                // If there is no entry in the executing map, then the execution has been canceled
                // and we don't need to send any message to the broker.
                if self.executing.remove(&id).is_some() {
                    self.deps
                        .send_response_to_broker(WorkerResponse(id, result));
                }
                self.possibly_start_execution();
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

impl From<WorkerRequest> for Message {
    fn from(request: WorkerRequest) -> Message {
        Message::FromBroker(request)
    }
}

impl<D: DispatcherDeps> Dispatcher<D> {
    fn possibly_start_execution(&mut self) {
        if self.executing.len() < self.slots {
            if let Some((id, details)) = self.blocked.pop_front() {
                let handle = self.deps.start_execution(id, details);
                if self.executing.insert(id, handle).is_some() {
                    panic!("duplicate id ${id:?}");
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
    use crate::test::*;
    use itertools::Itertools;
    use std::cell::RefCell;
    use std::rc::Rc;
    use WorkerRequest::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        StartExecution(ExecutionId, ExecutionDetails),
        DropExecutionHandle(ExecutionId),
        SendResponseToBroker(WorkerResponse),
    }

    use TestMessage::*;

    struct FakeState {
        messages: Vec<TestMessage>,
    }

    impl FakeState {
        fn new() -> Self {
            FakeState {
                messages: Vec::new(),
            }
        }
    }

    struct ExecutionHandle {
        id: ExecutionId,
        fake_state: Rc<RefCell<FakeState>>,
    }

    impl Drop for ExecutionHandle {
        fn drop(&mut self) {
            self.fake_state
                .borrow_mut()
                .messages
                .push(DropExecutionHandle(self.id))
        }
    }

    impl DispatcherDeps for Rc<RefCell<FakeState>> {
        type ExecutionHandle = ExecutionHandle;

        fn start_execution(
            &mut self,
            id: ExecutionId,
            details: ExecutionDetails,
        ) -> ExecutionHandle {
            self.borrow_mut().messages.push(StartExecution(id, details));
            ExecutionHandle {
                id,
                fake_state: self.clone(),
            }
        }

        fn send_response_to_broker(&mut self, message: WorkerResponse) {
            self.borrow_mut()
                .messages
                .push(SendResponseToBroker(message));
        }
    }

    struct Fixture {
        fake_state: Rc<RefCell<FakeState>>,
        dispatcher: Dispatcher<Rc<RefCell<FakeState>>>,
    }

    impl Fixture {
        fn new(slots: usize) -> Self {
            let fake_state = Rc::new(RefCell::new(FakeState::new()));
            let dispatcher = Dispatcher::new(fake_state.clone(), slots);
            Fixture {
                fake_state,
                dispatcher,
            }
        }

        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let messages = &mut self.fake_state.borrow_mut().messages;
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
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
    }

    script_test! {
        enqueue_2,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
    }

    script_test! {
        enqueue_3_with_2_slots,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
        FromBroker(EnqueueExecution(eid![3], details![3])) => {};
    }

    script_test! {
        complete_1,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromExecutor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerResponse(eid![1], result![1])),
        };
    }

    script_test! {
        complete_1_while_blocked,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
        FromBroker(EnqueueExecution(eid![3], details![3])) => {};
        FromExecutor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerResponse(eid![1], result![1])),
            StartExecution(eid![3], details![3]),
        };
    }

    script_test! {
        enqueue_2_complete_1_enqueue_1,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
        FromExecutor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerResponse(eid![1], result![1]))
        };
        FromBroker(EnqueueExecution(eid![3], details![3])) => { StartExecution(eid![3], details![3]) };
    }

    script_test! {
        cancel_queued,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
        FromBroker(EnqueueExecution(eid![3], details![3])) => {};
        FromBroker(EnqueueExecution(eid![4], details![4])) => {};
        FromBroker(CancelExecution(eid![3])) => {};
        FromExecutor(eid![1], result![1]) => {
            DropExecutionHandle(eid![1]),
            SendResponseToBroker(WorkerResponse(eid![1], result![1])),
            StartExecution(eid![4], details![4]),
        };
    }

    script_test! {
        cancel_executing,
        2,
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
        FromBroker(EnqueueExecution(eid![3], details![3])) => {};
        FromBroker(CancelExecution(eid![2])) => { DropExecutionHandle(eid![2]) };
        FromExecutor(eid![2], result![2]) => { StartExecution(eid![3], details![3]) };
    }

    script_test! {
        cancels_idempotent,
        2,
        FromBroker(CancelExecution(eid![2])) => {};
        FromBroker(CancelExecution(eid![2])) => {};
        FromBroker(EnqueueExecution(eid![1], details![1])) => { StartExecution(eid![1], details![1]) };
        FromBroker(EnqueueExecution(eid![2], details![2])) => { StartExecution(eid![2], details![2]) };
        FromBroker(EnqueueExecution(eid![3], details![3])) => {};
        FromBroker(EnqueueExecution(eid![4], details![4])) => {};
        FromBroker(CancelExecution(eid![4])) => {};
        FromBroker(CancelExecution(eid![4])) => {};
        FromBroker(CancelExecution(eid![4])) => {};
        FromBroker(CancelExecution(eid![2])) => { DropExecutionHandle(eid![2]) };
        FromBroker(CancelExecution(eid![2])) => {};
        FromBroker(CancelExecution(eid![2])) => {};
        FromExecutor(eid![2], result![2]) => { StartExecution(eid![3], details![3]) };
        FromBroker(CancelExecution(eid![2])) => {};
        FromBroker(CancelExecution(eid![2])) => {};
    }

    #[test]
    #[should_panic(expected = "assertion failed: slots > 0")]
    fn slots_must_be_nonzero() {
        let _ = Fixture::new(0);
    }

    #[test]
    #[should_panic(expected = "duplicate id")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(2);
        fixture
            .dispatcher
            .receive_message(FromBroker(EnqueueExecution(eid![1], details![1])));
        fixture
            .dispatcher
            .receive_message(FromBroker(EnqueueExecution(eid![1], details![2])));
    }
}
