use crate::{worker::ToDispatcher, ExecutionDetails, ExecutionId, WorkerRequest, WorkerResponse};
use claim::*;
use std::collections::{HashMap, VecDeque};

/// Manage executions based on the slot count and requests from the broker. Executes
/// requests in FIFO order: It's up to the broker to order the requests properly.
///
/// All methods are completely nonblocking. They will never block the task or the thread.
pub struct Dispatcher<D: DispatcherDeps> {
    deps: D,
    slots: u32,
    slots_used: u32,
    blocked: VecDeque<(ExecutionId, ExecutionDetails)>,
    executing: HashMap<ExecutionId, D::ExecutionHandle>,
}

/// The external dependencies for [Dispatcher]. All of these methods must be asynchronous: they
/// must not block the current task or thread.
pub trait DispatcherDeps {
    /// A handle to a pending execution. When dropped, the handle must tell the execution to
    /// terminate immediately. However, the drop method must not wait for the execution to
    /// terminate. Instead, that must happen asynchronously, and the actual execution termination
    /// notification must come through as a [ToDispatcher::FromExecutor] message.
    ///
    /// It must be safe to drop the handle after the execution has terminated.
    type ExecutionHandle;

    /// Start a new execution. When the execution terminates, the notification must come through as
    /// a [ToDispatcher::FromExecutor] message.
    fn start_execution(
        &mut self,
        id: ExecutionId,
        details: ExecutionDetails,
    ) -> Self::ExecutionHandle;

    /// Send a message to the broker.
    fn send_response_to_broker(&mut self, message: WorkerResponse);
}

impl<D: DispatcherDeps> Dispatcher<D> {
    /// Create a new dispatcher with the provided slot count. The slot count must be a positive
    /// number.
    pub fn new(deps: D, slots: u32) -> Self {
        assert_gt!(slots, 0);
        Dispatcher {
            deps,
            slots,
            slots_used: 0,
            blocked: VecDeque::new(),
            executing: HashMap::new(),
        }
    }

    /// Process an incoming message. Messages come from the broker and from executors. See
    /// [ToDispatcher] for more information.
    pub fn receive_message(&mut self, msg: ToDispatcher) {
        match msg {
            ToDispatcher::FromBroker(WorkerRequest::EnqueueExecution(id, details)) => {
                self.blocked.push_back((id, details));
                self.possibly_start_execution();
            }
            ToDispatcher::FromBroker(WorkerRequest::CancelExecution(id)) => {
                use std::mem::swap;

                // Remove from blocked queue.
                let mut blocked = VecDeque::new();
                swap(&mut blocked, &mut self.blocked);
                let (mut keep, cancel) = blocked.into_iter().partition(|x| x.0 != id);
                swap(&mut keep, &mut self.blocked);
                for (id, _) in cancel.iter() {
                    self.deps
                        .send_response_to_broker(WorkerResponse::ExecutionCanceled(*id))
                }

                // Remove execution handle from executing map. Removing it now will drop it,
                // which will tell the executor to signal the process. When the executor finishes,
                // we'll interpret the missing entry as an indication to send a canceled message
                // instead of a completed message. We only need to do this if we didn't already
                // find the id in the blocked queue.
                if cancel.is_empty() {
                    self.executing.remove(&id);
                }
            }
            ToDispatcher::FromExecutor(id, result) => {
                self.deps
                    .send_response_to_broker(match self.executing.remove(&id) {
                        None => WorkerResponse::ExecutionCanceled(id),
                        Some(_) => WorkerResponse::ExecutionCompleted(id, result),
                    });
                self.slots_used -= 1;
                self.possibly_start_execution();
            }
        }
    }

    fn possibly_start_execution(&mut self) {
        if self.slots_used < self.slots {
            if let Some((id, details)) = self.blocked.pop_front() {
                self.slots_used += 1;
                let handle = self.deps.start_execution(id, details);
                let old = self.executing.insert(id, handle);
                assert!(old.is_none(), "duplicate id ${id:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use crate::ExecutionResult;
    use itertools::Itertools;
    use std::cell::RefCell;
    use std::rc::Rc;
    use ToDispatcher::*;
    use WorkerRequest::*;
    use WorkerResponse::*;

    #[derive(Clone, Debug, PartialEq)]
    enum Message {
        StartExecution(ExecutionId, ExecutionDetails),
        DropExecutionHandle(ExecutionId),
        SendResponseToBroker(WorkerResponse),
    }

    use Message::*;

    struct FakeState {
        messages: Vec<Message>,
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
        fn new(slots: u32) -> Self {
            let fake_state = Rc::new(RefCell::new(FakeState::new()));
            let dispatcher = Dispatcher::new(fake_state.clone(), slots);
            Fixture {
                fake_state,
                dispatcher,
            }
        }

        fn expect_messages_in_any_order(&mut self, expected: Vec<Message>) {
            let messages = &mut self.fake_state.borrow_mut().messages;
            for perm in expected.clone().into_iter().permutations(expected.len()) {
                if perm == *messages {
                    messages.clear();
                    return;
                }
            }
            panic!(
                "Expected messages didn't match actual messages in any order. \
                 Expected: {expected:?}, actual: {messages:?}"
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
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
    }

    script_test! {
        enqueue_2,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
    }

    script_test! {
        enqueue_3_with_2_slots,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
        FromBroker(EnqueueExecution(id![3], details![3])) => {};
    }

    script_test! {
        complete_1,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromExecutor(id![1], result![1]) => {
            DropExecutionHandle(id![1]),
            SendResponseToBroker(ExecutionCompleted(id![1], result![1])),
        };
    }

    script_test! {
        complete_1_while_blocked,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
        FromBroker(EnqueueExecution(id![3], details![3])) => {};
        FromExecutor(id![1], result![1]) => {
            DropExecutionHandle(id![1]),
            SendResponseToBroker(ExecutionCompleted(id![1], result![1])),
            StartExecution(id![3], details![3]),
        };
    }

    script_test! {
        enqueue_2_complete_1_enqueue_1,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
        FromExecutor(id![1], result![1]) => {
            DropExecutionHandle(id![1]),
            SendResponseToBroker(ExecutionCompleted(id![1], result![1]))
        };
        FromBroker(EnqueueExecution(id![3], details![3])) => { StartExecution(id![3], details![3]) };
    }

    script_test! {
        cancel_queued,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
        FromBroker(EnqueueExecution(id![3], details![3])) => {};
        FromBroker(EnqueueExecution(id![4], details![4])) => {};
        FromBroker(CancelExecution(id![3])) => { SendResponseToBroker(ExecutionCanceled(id![3])) };
        FromExecutor(id![1], result![1]) => {
            DropExecutionHandle(id![1]),
            SendResponseToBroker(ExecutionCompleted(id![1], result![1])),
            StartExecution(id![4], details![4]),
        };
    }

    script_test! {
        cancel_executing,
        2,
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
        FromBroker(EnqueueExecution(id![3], details![3])) => {};
        FromBroker(CancelExecution(id![2])) => { DropExecutionHandle(id![2]) };
        FromExecutor(id![2], result![2]) => {
            SendResponseToBroker(ExecutionCanceled(id![2])),
            StartExecution(id![3], details![3]),
        };
    }

    script_test! {
        cancels_idempotent,
        2,
        FromBroker(CancelExecution(id![2])) => {};
        FromBroker(CancelExecution(id![2])) => {};
        FromBroker(EnqueueExecution(id![1], details![1])) => { StartExecution(id![1], details![1]) };
        FromBroker(EnqueueExecution(id![2], details![2])) => { StartExecution(id![2], details![2]) };
        FromBroker(EnqueueExecution(id![3], details![3])) => {};
        FromBroker(EnqueueExecution(id![4], details![4])) => {};
        FromBroker(CancelExecution(id![4])) => { SendResponseToBroker(ExecutionCanceled(id![4])) };
        FromBroker(CancelExecution(id![4])) => {};
        FromBroker(CancelExecution(id![4])) => {};
        FromBroker(CancelExecution(id![2])) => { DropExecutionHandle(id![2]) };
        FromBroker(CancelExecution(id![2])) => {};
        FromBroker(CancelExecution(id![2])) => {};
        FromExecutor(id![2], result![2]) => {
            SendResponseToBroker(ExecutionCanceled(id![2])),
            StartExecution(id![3], details![3]),
        };
        FromBroker(CancelExecution(id![2])) => {};
        FromBroker(CancelExecution(id![2])) => {};
    }

    #[test]
    #[should_panic(expected = "left > right")]
    fn slots_must_be_nonzero() {
        let _ = Fixture::new(0);
    }

    #[test]
    #[should_panic(expected = "duplicate id")]
    fn duplicate_ids_from_broker_panics() {
        let mut fixture = Fixture::new(2);
        fixture
            .dispatcher
            .receive_message(FromBroker(EnqueueExecution(id![1], details![1])));
        fixture
            .dispatcher
            .receive_message(FromBroker(EnqueueExecution(id![1], details![2])));
    }
}
