use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

pub mod broker;
mod channel_reader;
pub mod client;
mod proto;
mod task;
pub mod worker;

#[cfg(test)]
pub mod test;

pub type Error = anyhow::Error;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientId(u32);

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientExecutionId(u32);

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ExecutionId(ClientId, ClientExecutionId);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExecutionDetails {
    pub program: String,
    pub arguments: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExecutionResult {
    Exited(u8),
    Signalled(u8),
    Error(String),
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WorkerId(u32);

pub trait SchedulerDeps {
    type ExecutorId;
    type InstanceId;
    type InstanceDetails;

    fn start_test_execution(
        &mut self,
        executor: Self::ExecutorId,
        instance_id: Self::InstanceId,
        instance_details: &Self::InstanceDetails,
    );
}

#[derive(Debug)]
struct Executor<IdType, DetailsType> {
    slots_total: usize,
    executing_instances: HashMap<IdType, DetailsType>,
}

pub struct Scheduler<DT: SchedulerDeps> {
    deps: DT,
    slots_total: usize,
    slots_used: usize,
    blocked_instances: Vec<(DT::InstanceId, DT::InstanceDetails)>,
    executors: HashMap<DT::ExecutorId, Executor<DT::InstanceId, DT::InstanceDetails>>,
}

impl<DT: SchedulerDeps> Scheduler<DT>
where
    DT::ExecutorId: Clone + Eq + Hash,
    DT::InstanceId: Clone + Eq + Hash,
{
    pub fn new(deps: DT) -> Self {
        Scheduler {
            deps,
            slots_total: 0,
            slots_used: 0,
            blocked_instances: Vec::new(),
            executors: HashMap::new(),
        }
    }

    fn unblock_one(&mut self) {
        for (executor_id, executor) in self.executors.iter_mut() {
            if executor.executing_instances.len() < executor.slots_total {
                let (instance_id, instance_details) = self.blocked_instances.pop().unwrap();
                self.deps.start_test_execution(
                    executor_id.clone(),
                    instance_id.clone(),
                    &instance_details,
                );
                self.slots_used += 1;
                executor
                    .executing_instances
                    .insert(instance_id, instance_details);
                return;
            }
        }
        panic!("Expected at least one available slot.");
    }

    fn unblock_all_possible(&mut self) {
        while self.slots_used < self.slots_total && !self.blocked_instances.is_empty() {
            self.unblock_one();
        }
    }

    pub fn add_executor(&mut self, id: DT::ExecutorId, slot_count: usize) {
        if self
            .executors
            .insert(
                id,
                Executor {
                    slots_total: slot_count,
                    executing_instances: HashMap::new(),
                },
            )
            .is_some()
        {
            panic!("Duplicate executor ID.");
        }
        self.slots_total += slot_count;
        self.unblock_all_possible();
    }

    pub fn remove_executor(&mut self, executor_id: DT::ExecutorId) {
        let mut executor = self.executors.remove(&executor_id).unwrap();
        self.slots_total -= executor.slots_total;
        self.slots_used -= executor.executing_instances.len();
        self.blocked_instances
            .extend(executor.executing_instances.drain());
        self.unblock_all_possible();
    }

    pub fn add_tests(
        &mut self,
        tests: impl IntoIterator<Item = (DT::InstanceId, DT::InstanceDetails)>,
    ) {
        self.blocked_instances.extend(tests);
        self.unblock_all_possible();
    }

    pub fn receive_test_execution_done(
        &mut self,
        executor_id: DT::ExecutorId,
        instance_id: DT::InstanceId,
    ) {
        let executor = self.executors.get_mut(&executor_id).unwrap();
        executor.executing_instances.remove(&instance_id).unwrap();
        self.slots_used -= 1;
        self.unblock_all_possible();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::assert_lt;
    use claim::assert_none;
    use std::cell::RefCell;
    use std::collections::HashSet;

    type FakeExecutorId = u32;
    type FakeInstanceId = &'static str;
    type FakeInstanceDetails = (String, String);

    struct FakeExecutor {
        slots: usize,
        in_progress: HashMap<FakeInstanceId, FakeInstanceDetails>,
    }

    struct FakeState {
        executors: HashMap<FakeExecutorId, FakeExecutor>,
    }

    impl FakeState {
        fn new() -> FakeState {
            FakeState {
                executors: HashMap::new(),
            }
        }
    }

    fn make_details(id: FakeInstanceId) -> FakeInstanceDetails {
        return (format!("./{}", id), format!("arg_for_{}", id));
    }

    impl SchedulerDeps for &RefCell<FakeState> {
        type ExecutorId = FakeExecutorId;
        type InstanceId = FakeInstanceId;
        type InstanceDetails = FakeInstanceDetails;

        fn start_test_execution(
            &mut self,
            executor_id: Self::ExecutorId,
            instance_id: Self::InstanceId,
            instance_details: &Self::InstanceDetails,
        ) {
            assert_eq!(instance_details.clone(), make_details(instance_id));
            match self.borrow_mut().executors.get_mut(&executor_id) {
                None => panic!("Executor {} not found.", executor_id),
                Some(executor) => {
                    assert_lt!(executor.in_progress.len(), executor.slots as usize);
                    assert_none!(executor
                        .in_progress
                        .insert(instance_id, instance_details.clone()));
                }
            }
        }
    }

    struct Fixture<'a> {
        fake_state: &'a RefCell<FakeState>,
        scheduler: Scheduler<&'a RefCell<FakeState>>,
    }

    impl<'a> Fixture<'a> {
        fn new(fake_state: &'a RefCell<FakeState>) -> Fixture<'a> {
            Fixture {
                fake_state,
                scheduler: Scheduler::new(fake_state),
            }
        }

        fn get_executing(&self) -> HashSet<FakeInstanceId> {
            let mut all = HashSet::<FakeInstanceId>::new();
            for executor in self.fake_state.borrow().executors.values() {
                all.extend(executor.in_progress.keys());
            }
            all
        }

        fn assert_executing(&self, expected_spec: impl IntoIterator<Item = FakeInstanceId>) {
            let actual = self.get_executing();
            let expected = HashSet::from_iter(expected_spec);
            assert_eq!(actual, expected);
        }

        fn assert_subset_executing(
            &self,
            count: usize,
            expected_spec: impl IntoIterator<Item = FakeInstanceId>,
        ) -> HashSet<FakeInstanceId> {
            let actual = self.get_executing();
            let expected = HashSet::from_iter(expected_spec);
            assert_eq!(count, actual.len());
            assert!(actual.is_subset(&expected));
            actual
        }

        fn add_executor(&mut self, id: FakeExecutorId, slot_count: usize) {
            self.fake_state.borrow_mut().executors.insert(
                id,
                FakeExecutor {
                    slots: slot_count,
                    in_progress: HashMap::new(),
                },
            );
            self.scheduler.add_executor(id, slot_count);
        }

        fn add_tests(&mut self, test_ids: impl IntoIterator<Item = FakeInstanceId>) {
            let mut tests = Vec::<(FakeInstanceId, FakeInstanceDetails)>::new();
            for test_id in test_ids {
                tests.push((test_id, make_details(test_id)));
            }
            self.scheduler.add_tests(tests.drain(..))
        }

        fn find_executor_and_remove_instance(&self, instance_id: FakeInstanceId) -> FakeExecutorId {
            for (executor_id, executor) in self.fake_state.borrow_mut().executors.iter_mut() {
                if let Some(_) = executor.in_progress.remove(instance_id) {
                    return executor_id.clone();
                }
            }
            panic!("Didn't find instance {}", instance_id);
        }

        fn finish_test_execution(&mut self, instance_id: FakeInstanceId) {
            let executor_id = self.find_executor_and_remove_instance(instance_id);
            self.scheduler
                .receive_test_execution_done(executor_id, instance_id);
        }
    }

    #[test]
    fn test_adding_tests_with_sufficient_executor() {
        let fake_state = RefCell::new(FakeState::new());
        let mut fixture = Fixture::new(&fake_state);

        fixture.add_executor(1, 1000);

        fixture.add_tests(["test1", "test2"]);
        fixture.assert_executing(["test1", "test2"]);

        fixture.add_tests(["test3", "test4"]);
        fixture.assert_executing(["test1", "test2", "test3", "test4"]);
    }

    #[test]
    fn test_adding_tests_with_sufficient_executors() {
        let fake_state = RefCell::new(FakeState::new());
        let mut fixture = Fixture::new(&fake_state);

        fixture.add_executor(1, 1);
        fixture.add_executor(2, 3);

        fixture.add_tests(["test1", "test2"]);
        fixture.assert_executing(["test1", "test2"]);

        fixture.add_tests(["test3", "test4"]);
        fixture.assert_executing(["test1", "test2", "test3", "test4"]);
    }

    #[test]
    fn test_adding_executor_with_waiting_tests() {
        let fake_state = RefCell::new(FakeState::new());
        let mut fixture = Fixture::new(&fake_state);

        fixture.add_tests(["test1", "test2"]);

        fixture.add_executor(1, 2);

        fixture.assert_executing(["test1", "test2"]);
    }

    #[test]
    fn test_adding_more_tests_than_available_slots() {
        let fake_state = RefCell::new(FakeState::new());
        let mut fixture = Fixture::new(&fake_state);

        fixture.add_executor(1, 2);
        fixture.add_executor(2, 2);

        fixture.add_tests(["test1", "test2", "test3", "test4", "test5"]);

        let executing =
            fixture.assert_subset_executing(4, ["test1", "test2", "test3", "test4", "test5"]);
        let to_finish = executing.iter().next().unwrap();
        fixture.finish_test_execution(to_finish);

        let executing =
            fixture.assert_subset_executing(4, ["test1", "test2", "test3", "test4", "test5"]);
        assert!(!executing.contains(to_finish));
    }
}
