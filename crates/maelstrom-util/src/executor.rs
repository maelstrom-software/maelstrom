use crate::ext::OptionExt as _;
use indexmap::IndexMap;
use std::{fmt::Debug, hash::Hash, mem, num::NonZeroUsize};

pub enum StartResult<TagT, PartialT, OutputT> {
    InProgress,
    Expand {
        partial: PartialT,
        added_inputs: Vec<(TagT, Vec<TagT>)>,
    },
    Done(OutputT),
}

pub struct Graph<DepsT: Deps>(IndexMap<DepsT::Tag, Entry<DepsT>>);

pub trait Deps {
    type CompletedHandle;
    type Tag: Debug + Eq + Hash;
    type Partial;
    type Output;

    #[must_use]
    fn start(
        &mut self,
        tag: &Self::Tag,
        state: &Option<Self::Partial>,
        inputs: Vec<&Self::Output>,
    ) -> StartResult<Self::Tag, Self::Partial, Self::Output>;
    fn completed(&mut self, handle: Self::CompletedHandle, tag: &Self::Tag, output: &Self::Output);
}

enum State<DepsT: Deps> {
    NotStarted,
    WaitingOnInputs {
        lacking: NonZeroUsize,
        handles: Vec<DepsT::CompletedHandle>,
    },
    Running {
        handles: Vec<DepsT::CompletedHandle>,
    },
    Completed {
        output: DepsT::Output,
    },
}

struct Entry<DepsT: Deps> {
    in_edges: Vec<usize>,
    out_edges: Vec<usize>,
    partial: Option<DepsT::Partial>,
}

pub struct Executor<DepsT: Deps> {
    evaluations: Graph<DepsT>,
    states: Vec<State<DepsT>>,
}

impl<DepsT: Deps> Default for Executor<DepsT> {
    fn default() -> Self {
        Self {
            evaluations: Graph(Default::default()),
            states: Default::default(),
        }
    }
}

enum DeferredWork<DepsT: Deps> {
    Completed(DepsT::Output),
    Expand {
        partial: DepsT::Partial,
        added_inputs: Vec<(DepsT::Tag, Vec<DepsT::Tag>)>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Handle(usize);

impl<DepsT: Deps> Executor<DepsT> {
    pub fn get(&self, tag: &DepsT::Tag) -> Handle {
        Handle(self.evaluations.0.get_index_of(tag).unwrap())
    }

    pub fn add(&mut self, tag: DepsT::Tag) -> Handle {
        self.add_with_inputs(tag, [])
    }

    pub fn add_with_inputs(
        &mut self,
        tag: DepsT::Tag,
        inputs: impl IntoIterator<Item = DepsT::Tag>,
    ) -> Handle {
        Handle(self.add_with_inputs_inner(tag, inputs))
    }

    fn add_with_inputs_inner(
        &mut self,
        tag: DepsT::Tag,
        inputs: impl IntoIterator<Item = DepsT::Tag>,
    ) -> usize {
        match self.evaluations.0.get_index_of(&tag) {
            Some(index) => index,
            None => {
                self.states.push(State::NotStarted);
                let index = self.evaluations.0.len();
                let in_edges = inputs
                    .into_iter()
                    .map(|in_edge_tag| {
                        let (in_edge_index, _, in_edge_entry) = self
                            .evaluations
                            .0
                            .get_full_mut(&in_edge_tag)
                            .unwrap_or_else(|| {
                                panic!(
                                    "{tag:?} depends on {in_edge_tag:?}, which hasn't been added"
                                )
                            });
                        in_edge_entry.out_edges.push(index);
                        in_edge_index
                    })
                    .collect::<Vec<_>>();
                self.evaluations
                    .0
                    .insert(
                        tag,
                        Entry {
                            in_edges,
                            out_edges: Default::default(),
                            partial: Default::default(),
                        },
                    )
                    .assert_is_none();
                index
            }
        }
    }

    pub fn expand(
        &mut self,
        deps: &mut DepsT,
        tag: &DepsT::Tag,
        partial: DepsT::Partial,
        added_inputs: impl IntoIterator<Item = DepsT::Tag>,
    ) {
        let mut deferred = vec![];
        let index = self.evaluations.0.get_index_of(tag).unwrap();

        let added_in_edges = added_inputs
            .into_iter()
            .map(|in_edge_tag| {
                let (in_edge_index, _, in_edge_entry) = self
                    .evaluations
                    .0
                    .get_full_mut(&in_edge_tag)
                    .unwrap_or_else(|| {
                        panic!("{tag:?} depends on {in_edge_tag:?}, which hasn't been added")
                    });
                in_edge_entry.out_edges.push(index);
                in_edge_index
            })
            .collect();

        self.expand_inner(deps, index, partial, added_in_edges, &mut deferred);
        self.do_deferred_work(deps, &mut deferred);
    }

    fn expand_inner(
        &mut self,
        deps: &mut DepsT,
        index: usize,
        partial: DepsT::Partial,
        added_in_edges: Vec<usize>,
        deferred: &mut Vec<(usize, DeferredWork<DepsT>)>,
    ) {
        // This is fine because if we start an entry and it immediately returns, then that will end
        // up in `deferred`, and won't traverse the entry's out-edges immediately. When we do the
        // deferred work, we will have properly set up `lacking` (below).
        let lacking = added_in_edges
            .iter()
            .filter(|in_edge_index| {
                !Self::ensure_started_and_get_completed(
                    deps,
                    &self.evaluations,
                    &mut self.states,
                    **in_edge_index,
                    deferred,
                )
            })
            .count();
        let (_, entry) = self.evaluations.0.get_index_mut(index).unwrap();
        entry.partial = Some(partial);
        entry.in_edges.extend(added_in_edges);
        let State::Running { handles } = &mut self.states[index] else {
            panic!("unexpected state");
        };
        match NonZeroUsize::new(lacking) {
            Some(lacking) => {
                let handles = mem::take(handles);
                self.states[index] = State::WaitingOnInputs { lacking, handles };
            }
            None => {
                Self::start(deps, &self.evaluations, &mut self.states, index, deferred);
            }
        }
    }

    fn start(
        deps: &mut DepsT,
        evaluations: &Graph<DepsT>,
        states: &mut [State<DepsT>],
        index: usize,
        deferred: &mut Vec<(usize, DeferredWork<DepsT>)>,
    ) {
        let (tag, entry) = evaluations.0.get_index(index).unwrap();
        let inputs = entry
            .in_edges
            .iter()
            .map(|in_edge_index| {
                let State::Completed { ref output } = states[*in_edge_index] else {
                    panic!("unexpected state");
                };
                output
            })
            .collect();
        match deps.start(tag, &entry.partial, inputs) {
            StartResult::InProgress => {}
            StartResult::Done(output) => {
                deferred.push((index, DeferredWork::Completed(output)));
            }
            StartResult::Expand {
                partial,
                added_inputs,
            } => {
                deferred.push((
                    index,
                    DeferredWork::Expand {
                        partial,
                        added_inputs,
                    },
                ));
            }
        }
    }

    fn ensure_started_and_get_completed(
        deps: &mut DepsT,
        evaluations: &Graph<DepsT>,
        states: &mut [State<DepsT>],
        index: usize,
        deferred: &mut Vec<(usize, DeferredWork<DepsT>)>,
    ) -> bool {
        match states[index] {
            State::NotStarted => {
                let lacking = evaluations.0[index]
                    .in_edges
                    .iter()
                    .filter(|in_edge_index| {
                        !Self::ensure_started_and_get_completed(
                            deps,
                            evaluations,
                            states,
                            **in_edge_index,
                            deferred,
                        )
                    })
                    .count();
                let handles = Default::default();
                match NonZeroUsize::new(lacking) {
                    Some(lacking) => {
                        states[index] = State::WaitingOnInputs { lacking, handles };
                    }
                    None => {
                        states[index] = State::Running { handles };
                        Self::start(deps, evaluations, states, index, deferred);
                    }
                }
                false
            }
            State::WaitingOnInputs { .. } | State::Running { .. } => false,
            State::Completed { .. } => true,
        }
    }

    fn do_deferred_work(
        &mut self,
        deps: &mut DepsT,
        deferred: &mut Vec<(usize, DeferredWork<DepsT>)>,
    ) {
        while let Some((index, work)) = deferred.pop() {
            match work {
                DeferredWork::Completed(output) => {
                    let (tag, entry) = self.evaluations.0.get_index(index).unwrap();
                    Self::receive_completed_inner(
                        deps,
                        &self.evaluations,
                        &mut self.states,
                        index,
                        tag,
                        entry,
                        output,
                        deferred,
                    );
                }
                DeferredWork::Expand {
                    partial,
                    added_inputs,
                } => {
                    let added_in_edges = added_inputs
                        .into_iter()
                        .map(|(in_edge_tag, in_edge_inputs)| {
                            let in_edge_index =
                                self.add_with_inputs_inner(in_edge_tag, in_edge_inputs);
                            self.evaluations.0[in_edge_index].out_edges.push(index);
                            in_edge_index
                        })
                        .collect();
                    self.expand_inner(deps, index, partial, added_in_edges, deferred);
                }
            }
        }
    }

    pub fn evaluate(
        &mut self,
        deps: &mut DepsT,
        handle: Handle,
        completed_handle: DepsT::CompletedHandle,
    ) {
        let Handle(index) = handle;
        let mut deferred = vec![];
        Self::ensure_started_and_get_completed(
            deps,
            &self.evaluations,
            &mut self.states,
            index,
            &mut deferred,
        );
        self.do_deferred_work(deps, &mut deferred);
        match &mut self.states[index] {
            State::NotStarted => {
                panic!("unexpected state");
            }
            State::WaitingOnInputs { handles, .. } | State::Running { handles } => {
                handles.push(completed_handle);
            }
            State::Completed { output } => {
                let (tag, _) = self.evaluations.0.get_index(index).unwrap();
                deps.completed(completed_handle, tag, output);
            }
        }
    }

    pub fn receive_completed(&mut self, deps: &mut DepsT, tag: &DepsT::Tag, output: DepsT::Output) {
        let (index, _, entry) = self.evaluations.0.get_full(tag).unwrap();
        let mut deferred = vec![];
        Self::receive_completed_inner(
            deps,
            &self.evaluations,
            &mut self.states,
            index,
            tag,
            entry,
            output,
            &mut deferred,
        );
        self.do_deferred_work(deps, &mut deferred);
    }

    #[allow(clippy::too_many_arguments)]
    fn receive_completed_inner(
        deps: &mut DepsT,
        evaluations: &Graph<DepsT>,
        states: &mut [State<DepsT>],
        index: usize,
        tag: &DepsT::Tag,
        entry: &Entry<DepsT>,
        output: DepsT::Output,
        deferred: &mut Vec<(usize, DeferredWork<DepsT>)>,
    ) {
        let state = &mut states[index];
        let State::Running { handles: waiting } = mem::replace(state, State::Completed { output })
        else {
            panic!("unexpected state");
        };
        let State::Completed { output } = state else {
            panic!("unexpected state");
        };
        for handle in waiting {
            deps.completed(handle, tag, output);
        }

        for out_edge_index in &entry.out_edges {
            let out_edge_state = &mut states[*out_edge_index];
            let State::WaitingOnInputs { handles, lacking } = out_edge_state else {
                continue;
            };
            match NonZeroUsize::new(lacking.get() - 1) {
                Some(new_lacking) => {
                    *lacking = new_lacking;
                }
                None => {
                    *out_edge_state = State::Running {
                        handles: mem::take(handles),
                    };
                    Self::start(deps, evaluations, states, *out_edge_index, deferred);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::RefCell,
        collections::{HashMap, VecDeque},
        rc::Rc,
    };
    use TestMessage::*;

    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum TestMessage {
        Start(&'static str, Option<&'static str>, Vec<char>),
        Completed(i64, &'static str, char),
    }

    type TestStartResult = StartResult<&'static str, &'static str, char>;

    #[derive(Default)]
    struct TestState {
        messages: Vec<TestMessage>,
        start_results: HashMap<&'static str, VecDeque<TestStartResult>>,
    }

    impl Deps for Rc<RefCell<TestState>> {
        type Tag = &'static str;
        type CompletedHandle = i64;
        type Output = char;
        type Partial = &'static str;

        fn start(
            &mut self,
            tag: &Self::Tag,
            partial: &Option<Self::Partial>,
            inputs: Vec<&Self::Output>,
        ) -> StartResult<Self::Tag, Self::Partial, Self::Output> {
            let mut test_state = self.borrow_mut();
            test_state.messages.push(TestMessage::Start(
                tag,
                *partial,
                inputs.into_iter().copied().collect(),
            ));
            if let Some(results) = test_state.start_results.get_mut(tag) {
                if let Some(front) = results.pop_front() {
                    front
                } else {
                    StartResult::InProgress
                }
            } else {
                StartResult::InProgress
            }
        }

        fn completed(
            &mut self,
            handle: Self::CompletedHandle,
            tag: &Self::Tag,
            output: &Self::Output,
        ) {
            self.borrow_mut()
                .messages
                .push(TestMessage::Completed(handle, tag, *output));
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        sut: Executor<Rc<RefCell<TestState>>>,
    }

    impl Default for Fixture {
        fn default() -> Self {
            Self {
                test_state: Rc::new(RefCell::new(Default::default())),
                sut: Executor::default(),
            }
        }
    }

    impl Fixture {
        fn new(
            start_results_in: impl IntoIterator<Item = (&'static str, TestStartResult)>,
        ) -> Self {
            let mut start_results: HashMap<_, VecDeque<_>> = HashMap::new();
            for (tag, start_result) in start_results_in {
                start_results
                    .entry(tag)
                    .or_default()
                    .push_back(start_result);
            }
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Default::default(),
                start_results,
            }));
            Self {
                test_state,
                sut: Executor::default(),
            }
        }

        fn call_method(
            &mut self,
            method: impl FnOnce(&mut Executor<Rc<RefCell<TestState>>>, &mut Rc<RefCell<TestState>>),
        ) {
            method(&mut self.sut, &mut self.test_state);
        }

        fn expect_messages_in_any_order(&mut self, mut expected: Vec<TestMessage>) {
            expected.sort();
            let messages = &mut self.test_state.borrow_mut().messages;
            messages.sort();
            if expected == *messages {
                messages.clear();
                return;
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n\
                Expected: {expected:#?}\n\
                Actual: {messages:#?}\n\
                Diff: {}",
                colored_diff::PrettyDifference {
                    expected: &format!("{expected:#?}"),
                    actual: &format!("{messages:#?}")
                }
            );
        }
    }

    macro_rules! script_test {
        ($test_name:ident, $fixture:expr, $($call_method:expr => { $($out_msg:expr),* $(,)? });* $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture;
                $(
                    fixture.call_method($call_method);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )*
            }
        };
    }

    script_test! {
        no_dependencies,
        Fixture::default(),
        |e, d| {
            let handle = e.add("a");
            e.evaluate(d, handle, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, d| {
            let handle = e.get(&"a");
            e.evaluate(d, handle, 2);
        } => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
        |e, d| {
            let handle = e.get(&"a");
            e.evaluate(d, handle, 3);
        } => {
            Completed(3, "a", 'a'),
        };
    }

    script_test! {
        no_dependencies_immediate,
        Fixture::new([
            ("a", StartResult::Done('a')),
        ]),
        |e, d| {
            let handle = e.add("a");
            e.evaluate(d, handle, 1);
        } => {
            Start("a", None, vec![]),
            Completed(1, "a", 'a'),
        };
        |e, d| {
            let handle = e.get(&"a");
            e.evaluate(d, handle, 2);
        } => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        adding_multiple_times,
        Fixture::default(),
        |e, d| {
            let handle = e.add("a");
            let handle2 = e.add("a");
            assert_eq!(handle, handle2);
            e.evaluate(d, handle, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, _| {
            e.add("a");
        } => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
        |e, _| {
            e.add("a");
        } => {};
        |e, d| {
            let handle = e.get(&"a");
            e.evaluate(d, handle, 2);
        } => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        inputs,
        Fixture::default(),
        |e, _| {
            e.add("e");
            e.add_with_inputs("d", ["e"]);
            e.add("c");
            e.add_with_inputs("b", ["d", "c"]);
            e.add_with_inputs("a", ["b", "c"]);
        } => {};
        |e, d| {
            let a = e.get(&"a");
            e.evaluate(d, a, 1);
        } => {
            Start("c", None, vec![]),
            Start("e", None, vec![]),
        };
        |e, d| {
            let a = e.get(&"a");
            e.evaluate(d, a, 2);
        } => {};
        |e, d| {
            let b = e.get(&"b");
            e.evaluate(d, b, 3);
        } => {};
        |e, d| e.receive_completed(d, &"e", 'e') => {
            Start("d", None, vec!['e']),
        };
        |e, d| e.receive_completed(d, &"c", 'c') => {};
        |e, d| e.receive_completed(d, &"d", 'd') => {
            Start("b", None, vec!['d', 'c']),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Start("a", None, vec!['b', 'c']),
            Completed(3, "b", 'b'),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        inputs_immediate,
        Fixture::new([
            ("a", StartResult::Done('a')),
            ("b", StartResult::Done('b')),
            ("c", StartResult::Done('c')),
            ("d", StartResult::Done('d')),
            ("e", StartResult::Done('e')),
        ]),
        |e, _| {
            e.add("e");
            e.add_with_inputs("d", ["e"]);
            e.add("c");
            e.add_with_inputs("b", ["d", "c"]);
            e.add_with_inputs("a", ["b", "c"]);
        } => {};
        |e, d| {
            let a = e.get(&"a");
            e.evaluate(d, a, 1);
        } => {
            Start("a", None, vec!['b', 'c']),
            Start("b", None, vec!['d', 'c']),
            Start("c", None, vec![]),
            Start("d", None, vec!['e']),
            Start("e", None, vec![]),
            Completed(1, "a", 'a'),
        };
        |e, d| {
            let a = e.get(&"a");
            e.evaluate(d, a, 2);
        } => {
            Completed(2, "a", 'a'),
        };
        |e, d| {
            let b = e.get(&"b");
            e.evaluate(d, b, 3);
        } => {
            Completed(3, "b", 'b'),
        };
    }

    script_test! {
        expand_new_dependency_not_started,
        Fixture::default(),
        |e, d| {
            let a = e.add("a");
            e.evaluate(d, a, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, d| {
            e.add("b");
            e.expand(d, &"a", "partial-a-1", ["b"]);
        } => {
            Start("b", None, vec![]),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Start("a", Some("partial-a-1"), vec!['b']),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
    }

    script_test! {
        expand_new_dependency_not_started_immediate,
        Fixture::new([
            ("a", StartResult::Expand {
                partial: "partial-a-1",
                added_inputs: vec![("b", vec![])],
            }),
        ]),
        |e, d| {
            let a = e.add("a");
            e.evaluate(d, a, 1);
        } => {
            Start("a", None, vec![]),
            Start("b", None, vec![]),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Start("a", Some("partial-a-1"), vec!['b']),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
    }

    script_test! {
        expand_new_dependency_started,
        Fixture::default(),
        |e, d| {
            let a = e.add("a");
            e.evaluate(d, a, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, d| {
            let b = e.add("b");
            e.evaluate(d, b, 2);
        } => {
            Start("b", None, vec![]),
        };
        |e, d| e.expand(d, &"a", "partial-a-1", ["b"]) => {};
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Completed(2, "b", 'b'),
            Start("a", Some("partial-a-1"), vec!['b']),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
    }

    script_test! {
        expand_new_dependency_completed,
        Fixture::default(),
        |e, d| {
            let a = e.add("a");
            e.evaluate(d, a, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, d| {
            let b = e.add("b");
            e.evaluate(d, b, 2);
        } => {
            Start("b", None, vec![]),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Completed(2, "b", 'b'),
        };
        |e, d| e.expand(d, &"a", "partial-a-1", ["b"]) => {
            Start("a", Some("partial-a-1"), vec!['b']),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
    }

    script_test! {
        expand_new_dependency_cycle,
        Fixture::default(),
        |e, d| {
            e.add("a");
            let b = e.add_with_inputs("b", ["a"]);
            e.evaluate(d, b, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, d| e.expand(d, &"a", "partial-a-1", ["b"]) => {};
    }

    script_test! {
        evaluate_after_dependency_completed,
        Fixture::default(),
        |e, d| {
            e.add("a");
            let b = e.add_with_inputs("b", ["a"]);
            e.add_with_inputs("c", ["a"]);
            e.evaluate(d, b, 1);
        } => {
            Start("a", None, vec![]),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Start("b", None, vec!['a']),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Completed(1, "b", 'b'),
        };
        |e, d| {
            let c = e.get(&"c");
            e.evaluate(d, c, 2);
        } => {
            Start("c", None, vec!['a']),
        };
        |e, d| e.receive_completed(d, &"c", 'c') => {
            Completed(2, "c", 'c'),
        };
    }
}
