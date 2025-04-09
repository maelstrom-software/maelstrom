use crate::ext::OptionExt as _;
use indexmap::IndexMap;
use std::{fmt::Debug, hash::Hash, mem, num::NonZeroUsize};

/// The dependencies that an [`Executor`] has on its environment. These must be provided by a
/// client of the `Executor`.
pub trait Deps {
    /// An instance of this type is provided to the `Executor` when an evaluation is started. The
    /// `Executor` in turn provides it back to the client when an evaluation completes (via
    /// the [`Deps::completed`] callback).
    type CompletedHandle;

    /// The "key" as it were of an evaluation. This must uniquely specify an evaluation. At most
    /// one evaluation will be run for a given tag. The completed value for the first evaluation
    /// will be used for any subsequent evaluations with the same tag.
    type Tag: Clone + Debug + Eq + Hash;

    /// The state of a partially completed evaluation. This is provided by [`StartResult::Expand`]
    /// and then passed to [`Deps::completed`].
    type Partial;

    /// The "return value" type of an evaluation. This is returned by [`Executor::evaluation`], and
    /// is also provided as an input to other evaluations.
    type Output;

    /// Start or continue executing an evaluation. If `partial` is `None`, then this is the first
    /// time that this method will have been called for the provided `tag`. On the other hande, if
    /// `partial` is `Some`, then the value will have been provided by a previous
    /// [`StartResult::Expand`].
    fn start(
        &mut self,
        tag: Self::Tag,
        partial: Option<Self::Partial>,
        inputs: Vec<&Self::Output>,
        graph: &mut Graph<Self>,
    ) -> StartResult<Self::Partial, Self::Output>;

    /// Notify the client that an evaluation has completed. This may be called immediately
    /// (recursively) from [`Executor::evaluate`], or later from [`Executor::receive_completed`].
    fn completed(&mut self, handle: Self::CompletedHandle, tag: &Self::Tag, output: &Self::Output);
}

/// Return type for [`Deps::start`].
pub enum StartResult<PartialT, OutputT> {
    /// The evaluation has been started but will complete at some later time. When the evaluation
    /// completes, [`Executor::receive_completed`] will be called.
    InProgress,

    /// The evaluation has more inputs that need to be completed before it can complete. The
    /// `Executor` must ensure the added inputs are complete before calling [`Deps::start`] again.
    /// When it does, it will be called with the provided `partial`.
    Expand {
        partial: PartialT,
        added_inputs: Vec<Handle>,
    },

    /// The evaluation has completed immediately. There will be no call to
    /// [`Executor::receive_completed`].
    Completed(OutputT),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Handle(usize);

#[derive(Default)]
struct Entry {
    in_edges: Vec<usize>,
    out_edges: Vec<usize>,
}

/// A type that can be used to add dependencies from within [`Deps::start`],
/// [`Executor::evaluate`], and [`Executor::evaluate_with_inputs`].
pub struct Graph<DepsT: Deps + ?Sized>(IndexMap<DepsT::Tag, Entry>);

impl<DepsT: Deps + ?Sized> Graph<DepsT> {
    /// Add the provided `tag` to the graph and return a [`Handle`] pointing to it. If the `tag`
    /// has already been added, just return its `Handle`.
    pub fn add(&mut self, tag: DepsT::Tag) -> Handle {
        self.add_with_inputs(tag, |_| [])
    }

    /// Add the provided `tag` to the graph and return a [`Handle`] pointing to it. If the `tag`
    /// has already been added, just return its `Handle`. Otherwise, call `f` to get a list of
    /// inputs, and then add the `tag` with those dependencies.
    pub fn add_with_inputs<F, I>(&mut self, tag: DepsT::Tag, f: F) -> Handle
    where
        F: FnOnce(&mut Self) -> I,
        I: IntoIterator<Item = Handle>,
    {
        Handle(self.add_with_inputs_inner(tag, f))
    }

    fn add_with_inputs_inner<F, I>(&mut self, tag: DepsT::Tag, f: F) -> usize
    where
        F: FnOnce(&mut Self) -> I,
        I: IntoIterator<Item = Handle>,
    {
        match self.0.get_index_of(&tag) {
            Some(index) => index,
            None => {
                // We have to be careful about ordering here. We need to actually allocate our
                // entry in order to get its index, instead of just predicting the index. This is
                // because calling `inputs` may allocate other entries, which would mess up our
                // prediction. So what we do is this two-part insert seen here.
                //
                // Another (less efficient) option is to first compute the in-edges, then insert
                // the entry with a cloned copy, and then visit the in-edges again and update their
                // out-edges.
                let (index, old_value) = self.0.insert_full(tag, Default::default());
                old_value.assert_is_none();
                self.0[index].in_edges = f(self)
                    .into_iter()
                    .map(|Handle(in_edge_index)| {
                        self.0[in_edge_index].out_edges.push(index);
                        in_edge_index
                    })
                    .collect();
                index
            }
        }
    }
}

enum DeferredWork<DepsT: Deps> {
    Completed {
        index: usize,
        output: DepsT::Output,
    },
    Expand {
        index: usize,
        partial: DepsT::Partial,
        added_inputs: Vec<Handle>,
    },
}

enum State<DepsT: Deps> {
    NotStarted,
    WaitingOnInputs {
        lacking: NonZeroUsize,
        handles: Vec<DepsT::CompletedHandle>,
        partial: Option<DepsT::Partial>,
    },
    Running {
        handles: Vec<DepsT::CompletedHandle>,
    },
    Completed {
        output: DepsT::Output,
    },
}

pub struct Executor<DepsT: Deps> {
    graph: Graph<DepsT>,
    states: Vec<State<DepsT>>,
}

impl<DepsT: Deps> Default for Executor<DepsT> {
    fn default() -> Self {
        Self {
            graph: Graph(Default::default()),
            states: Default::default(),
        }
    }
}

impl<DepsT: Deps> Executor<DepsT> {
    pub fn evaluate(
        &mut self,
        deps: &mut DepsT,
        completed_handle: DepsT::CompletedHandle,
        tag: DepsT::Tag,
    ) {
        self.evaluate_with_inputs(deps, completed_handle, tag, |_| []);
    }

    pub fn evaluate_with_inputs<F, I>(
        &mut self,
        deps: &mut DepsT,
        completed_handle: DepsT::CompletedHandle,
        tag: DepsT::Tag,
        inputs: F,
    ) where
        F: FnOnce(&mut Graph<DepsT>) -> I,
        I: IntoIterator<Item = Handle>,
    {
        let Handle(index) = self.graph.add_with_inputs(tag, inputs);
        let mut deferred = vec![];
        self.ensure_started_and_get_completed(deps, index, &mut deferred);
        self.do_deferred_work(deps, &mut deferred);
        match &mut self.states[index] {
            State::NotStarted => {
                panic!("unexpected state");
            }
            State::WaitingOnInputs { handles, .. } | State::Running { handles } => {
                handles.push(completed_handle);
            }
            State::Completed { output } => {
                let (tag, _) = self.graph.0.get_index(index).unwrap();
                deps.completed(completed_handle, tag, output);
            }
        }
    }

    pub fn receive_completed(&mut self, deps: &mut DepsT, tag: &DepsT::Tag, output: DepsT::Output) {
        let index = self.graph.0.get_index_of(tag).unwrap();
        let mut deferred = vec![];
        self.receive_completed_inner(deps, index, output, &mut deferred);
        self.do_deferred_work(deps, &mut deferred);
    }

    fn ensure_started_and_get_completed(
        &mut self,
        deps: &mut DepsT,
        index: usize,
        deferred: &mut Vec<DeferredWork<DepsT>>,
    ) -> bool {
        match self.get_state_mut(index) {
            State::NotStarted => {
                let in_edges = self.graph.0[index].in_edges.clone();
                let lacking = in_edges
                    .into_iter()
                    .filter(|in_edge_index| {
                        !self.ensure_started_and_get_completed(deps, *in_edge_index, deferred)
                    })
                    .count();
                let handles = Default::default();
                let partial = None;
                match NonZeroUsize::new(lacking) {
                    Some(lacking) => {
                        self.set_state(
                            index,
                            State::WaitingOnInputs {
                                lacking,
                                handles,
                                partial,
                            },
                        );
                    }
                    None => {
                        self.set_state(index, State::Running { handles });
                        self.start(deps, index, deferred, partial);
                    }
                }
                false
            }
            State::WaitingOnInputs { .. } | State::Running { .. } => false,
            State::Completed { .. } => true,
        }
    }

    fn do_deferred_work(&mut self, deps: &mut DepsT, deferred: &mut Vec<DeferredWork<DepsT>>) {
        while let Some(work) = deferred.pop() {
            match work {
                DeferredWork::Completed { index, output } => {
                    self.receive_completed_inner(deps, index, output, deferred);
                }
                DeferredWork::Expand {
                    index,
                    partial,
                    added_inputs,
                } => {
                    let added_in_edges = added_inputs
                        .into_iter()
                        .map(|Handle(in_edge_index)| {
                            self.graph.0[in_edge_index].out_edges.push(index);
                            in_edge_index
                        })
                        .collect();
                    self.expand(deps, index, partial, added_in_edges, deferred);
                }
            }
        }
    }

    fn receive_completed_inner(
        &mut self,
        deps: &mut DepsT,
        index: usize,
        output: DepsT::Output,
        deferred: &mut Vec<DeferredWork<DepsT>>,
    ) {
        let state = &mut self.states[index];
        let State::Running { handles: waiting } = mem::replace(state, State::Completed { output })
        else {
            panic!("unexpected state");
        };
        let State::Completed { output } = state else {
            panic!("unexpected state");
        };

        let (tag, entry) = self.graph.0.get_index(index).unwrap();
        for handle in waiting {
            deps.completed(handle, tag, output);
        }

        let out_edges = entry.out_edges.clone();
        for out_edge_index in out_edges {
            let out_edge_state = self.get_state_mut(out_edge_index);
            let State::WaitingOnInputs {
                handles,
                lacking,
                partial,
            } = out_edge_state
            else {
                continue;
            };
            match NonZeroUsize::new(lacking.get() - 1) {
                Some(new_lacking) => {
                    *lacking = new_lacking;
                }
                None => {
                    let partial = partial.take();
                    *out_edge_state = State::Running {
                        handles: mem::take(handles),
                    };
                    self.start(deps, out_edge_index, deferred, partial);
                }
            }
        }
    }

    fn start(
        &mut self,
        deps: &mut DepsT,
        index: usize,
        deferred: &mut Vec<DeferredWork<DepsT>>,
        partial: Option<DepsT::Partial>,
    ) {
        let (tag, entry) = self.graph.0.get_index(index).unwrap();
        let inputs = entry
            .in_edges
            .iter()
            .map(|in_edge_index| {
                let State::Completed { ref output } = self.states[*in_edge_index] else {
                    panic!("unexpected state");
                };
                output
            })
            .collect();
        match deps.start(tag.clone(), partial, inputs, &mut self.graph) {
            StartResult::InProgress => {}
            StartResult::Completed(output) => {
                deferred.push(DeferredWork::Completed { index, output });
            }
            StartResult::Expand {
                partial,
                added_inputs,
            } => {
                deferred.push(DeferredWork::Expand {
                    index,
                    partial,
                    added_inputs,
                });
            }
        }
    }

    fn expand(
        &mut self,
        deps: &mut DepsT,
        index: usize,
        partial: DepsT::Partial,
        added_in_edges: Vec<usize>,
        deferred: &mut Vec<DeferredWork<DepsT>>,
    ) {
        // This is fine because if we start an entry and it immediately returns, then that will end
        // up in `deferred`, and won't traverse the entry's out-edges immediately. When we do the
        // deferred work, we will have properly set up `lacking` (below).
        let lacking = added_in_edges
            .iter()
            .filter(|in_edge_index| {
                !self.ensure_started_and_get_completed(deps, **in_edge_index, deferred)
            })
            .count();
        let (_, entry) = self.graph.0.get_index_mut(index).unwrap();
        entry.in_edges.extend(added_in_edges);
        let state = self.get_state_mut(index);
        let State::Running { handles } = state else {
            panic!("unexpected state");
        };
        match NonZeroUsize::new(lacking) {
            Some(lacking) => {
                let handles = mem::take(handles);
                *state = State::WaitingOnInputs {
                    lacking,
                    handles,
                    partial: Some(partial),
                };
            }
            None => {
                self.start(deps, index, deferred, Some(partial));
            }
        }
    }

    fn set_state(&mut self, index: usize, state: State<DepsT>) {
        *self.get_state_mut(index) = state;
    }

    fn get_state_mut(&mut self, index: usize) -> &mut State<DepsT> {
        if index >= self.states.len() {
            self.states.resize_with(index + 1, || State::NotStarted);
        }
        self.states.get_mut(index).unwrap()
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

    enum TestStartResult {
        Completed(char),
        Expand {
            partial: &'static str,
            added_inputs: Vec<&'static str>,
        },
    }

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
            tag: Self::Tag,
            partial: Option<Self::Partial>,
            inputs: Vec<&Self::Output>,
            graph: &mut Graph<Self>,
        ) -> StartResult<Self::Partial, Self::Output> {
            let mut test_state = self.borrow_mut();
            test_state.messages.push(TestMessage::Start(
                tag,
                partial,
                inputs.into_iter().copied().collect(),
            ));
            if let Some(results) = test_state.start_results.get_mut(tag) {
                match results.pop_front() {
                    Some(TestStartResult::Completed(output)) => StartResult::Completed(output),
                    Some(TestStartResult::Expand {
                        partial,
                        added_inputs,
                    }) => StartResult::Expand {
                        partial,
                        added_inputs: added_inputs
                            .into_iter()
                            .map(|added_input| graph.add(added_input))
                            .collect(),
                    },
                    None => StartResult::InProgress,
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

        fn call_method<F, T>(&mut self, method: F)
        where
            F: FnOnce(&mut Executor<Rc<RefCell<TestState>>>, &mut Rc<RefCell<TestState>>) -> T,
        {
            let _ = method(&mut self.sut, &mut self.test_state);
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
        |e, d| e.evaluate(d, 1, "a") => {
            Start("a", None, vec![]),
        };
        |e, d| e.evaluate(d, 2, "a") => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
        |e, d| e.evaluate(d, 3, "a") => {
            Completed(3, "a", 'a'),
        };
    }

    script_test! {
        no_dependencies_immediate,
        Fixture::new([
            ("a", TestStartResult::Completed('a')),
        ]),
        |e, d| e.evaluate(d, 1, "a") => {
            Start("a", None, vec![]),
            Completed(1, "a", 'a'),
        };
        |e, d| e.evaluate(d, 2, "a") => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        adding_multiple_times,
        Fixture::default(),
        |e, d| e.evaluate(d, 1, "a") => {
            Start("a", None, vec![]),
        };
        |e, d| e.evaluate(d, 2, "a") => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
        |e, d| e.evaluate(d, 3, "a") => {
            Completed(3, "a", 'a'),
        };
    }

    script_test! {
        inputs,
        Fixture::default(),
        |e, d| e.evaluate_with_inputs(
            d, 1, "a", |graph| {
                let c = graph.add("c");
                let b = graph.add_with_inputs("b", |graph| [
                    graph.add_with_inputs("d", |graph| [graph.add("e")]),
                    c,
                ]);
                [b, c]
            },
        ) => {
            Start("c", None, vec![]),
            Start("e", None, vec![]),
        };
        |e, d| e.evaluate(d, 2, "a") => {};
        |e, d| e.evaluate(d, 3, "b") => {};
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
            ("a", TestStartResult::Completed('a')),
            ("b", TestStartResult::Completed('b')),
            ("c", TestStartResult::Completed('c')),
            ("d", TestStartResult::Completed('d')),
            ("e", TestStartResult::Completed('e')),
        ]),
        |e, d| e.evaluate_with_inputs(
            d, 1, "a", |graph| {
                let c = graph.add("c");
                let b = graph.add_with_inputs("b", |graph| [
                    graph.add_with_inputs("d", |graph| [graph.add("e")]),
                    c,
                ]);
                [b, c]
            },
        ) => {
            Start("a", None, vec!['b', 'c']),
            Start("b", None, vec!['d', 'c']),
            Start("c", None, vec![]),
            Start("d", None, vec!['e']),
            Start("e", None, vec![]),
            Completed(1, "a", 'a'),
        };
        |e, d| e.evaluate(d, 2, "a") => {
            Completed(2, "a", 'a'),
        };
        |e, d| e.evaluate(d, 3, "b") => {
            Completed(3, "b", 'b'),
        };
    }

    script_test! {
        expand_new_dependency_not_started_immediate,
        Fixture::new([
            ("a", TestStartResult::Expand {
                partial: "partial-a-1",
                added_inputs: vec!["b"],
            }),
        ]),
        |e, d| e.evaluate(d, 1, "a") => {
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
        expand_new_dependency_completed,
        Fixture::new([(
            "a",
            TestStartResult::Expand {
                partial: "partial-a-1",
                added_inputs: vec!["b"],
            },
        )]),
        |e, d| e.evaluate(d, 1, "b") => {
            Start("b", None, vec![]),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Completed(1, "b", 'b'),
        };
        |e, d| e.evaluate(d, 2, "a") => {
            Start("a", None, vec![]),
            Start("a", Some("partial-a-1"), vec!['b']),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        expand_new_dependency_cycle,
        Fixture::new([(
            "a",
            TestStartResult::Expand {
                partial: "partial-a-1",
                added_inputs: vec!["b"],
            },
        )]),
        |e, d| e.evaluate_with_inputs(d, 1, "b", |graph| [graph.add("a")]) => {
            Start("a", None, vec![]),
        };
    }

    script_test! {
        evaluate_after_dependency_completed,
        Fixture::default(),
        |e, d| e.evaluate_with_inputs(d, 1, "b", |graph| [graph.add("a")]) => {
            Start("a", None, vec![]),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Start("b", None, vec!['a']),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Completed(1, "b", 'b'),
        };
        |e, d| e.evaluate_with_inputs(d, 2, "c", |graph| [graph.add("a")]) => {
            Start("c", None, vec!['a']),
        };
        |e, d| e.receive_completed(d, &"c", 'c') => {
            Completed(2, "c", 'c'),
        };
    }
}
