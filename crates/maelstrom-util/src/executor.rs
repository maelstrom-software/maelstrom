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
    evaluations: IndexMap<DepsT::Tag, Entry<DepsT>>,
    states: Vec<State<DepsT>>,
}

impl<DepsT: Deps> Default for Executor<DepsT> {
    fn default() -> Self {
        Self {
            evaluations: Default::default(),
            states: Default::default(),
        }
    }
}

impl<DepsT: Deps> Executor<DepsT> {
    pub fn add(&mut self, tag: DepsT::Tag) {
        self.add_with_inputs(tag, []);
    }

    pub fn add_with_inputs(
        &mut self,
        tag: DepsT::Tag,
        inputs: impl IntoIterator<Item = DepsT::Tag>,
    ) {
        if !self.evaluations.contains_key(&tag) {
            self.states.push(State::NotStarted);
            let index = self.evaluations.len();
            let in_edges = inputs
                .into_iter()
                .map(|in_edge_tag| {
                    let (in_edge_index, _, entry) = self
                        .evaluations
                        .get_full_mut(&in_edge_tag)
                        .unwrap_or_else(|| {
                            panic!("{tag:?} depends on {in_edge_tag:?}, which hasn't been added")
                        });
                    entry.out_edges.push(index);
                    in_edge_index
                })
                .collect::<Vec<_>>();
            self.evaluations
                .insert(
                    tag,
                    Entry {
                        in_edges,
                        out_edges: Default::default(),
                        partial: Default::default(),
                    },
                )
                .assert_is_none();
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
        self.expand_inner(deps, tag, partial, added_inputs, &mut deferred);
        self.do_deferred_work(deps, &mut deferred);
    }

    fn expand_inner(
        &mut self,
        deps: &mut DepsT,
        tag: &DepsT::Tag,
        partial: DepsT::Partial,
        added_inputs: impl IntoIterator<Item = DepsT::Tag>,
        deferred: &mut Vec<(usize, DepsT::Output)>,
    ) {
        let added_in_edges = added_inputs
            .into_iter()
            .map(|in_edge_tag| {
                self.evaluations
                    .get_index_of(&in_edge_tag)
                    .unwrap_or_else(|| {
                        panic!("{tag:?} depends on {in_edge_tag:?}, which hasn't been added")
                    })
            })
            .collect::<Vec<_>>();
        let (index, _, entry) = self.evaluations.get_full_mut(tag).unwrap();
        entry.partial = Some(partial);
        entry.in_edges.append(&mut added_in_edges.clone());
        for in_edge_index in &added_in_edges {
            self.evaluations[*in_edge_index].out_edges.push(index);
        }
        let lacking = added_in_edges
            .into_iter()
            .filter(|in_edge_index| {
                !Self::ensure_started_and_get_completed(
                    deps,
                    &self.evaluations,
                    &mut self.states,
                    *in_edge_index,
                    deferred,
                )
            })
            .count();
        let State::Running { handles } = &mut self.states[index] else {
            panic!("unexpected state");
        };
        match NonZeroUsize::new(lacking) {
            Some(lacking) => {
                let handles = mem::take(handles);
                self.states[index] = State::WaitingOnInputs { lacking, handles };
            }
            None => {
                Self::start(
                    deps,
                    &mut self.states,
                    index,
                    tag,
                    &self.evaluations[index],
                    deferred,
                );
            }
        }
    }

    fn start(
        deps: &mut DepsT,
        states: &mut [State<DepsT>],
        index: usize,
        tag: &DepsT::Tag,
        entry: &Entry<DepsT>,
        deferred: &mut Vec<(usize, DepsT::Output)>,
    ) {
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
                deferred.push((index, output));
            }
            StartResult::Expand { .. } => {
                todo!();
            }
        }
    }

    fn ensure_started_and_get_completed(
        deps: &mut DepsT,
        evaluations: &IndexMap<DepsT::Tag, Entry<DepsT>>,
        states: &mut [State<DepsT>],
        index: usize,
        deferred: &mut Vec<(usize, DepsT::Output)>,
    ) -> bool {
        let (tag, entry) = evaluations.get_index(index).unwrap();
        match states[index] {
            State::NotStarted => {
                let lacking = entry
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
                        Self::start(deps, states, index, tag, entry, deferred);
                    }
                }
                false
            }
            State::WaitingOnInputs { .. } | State::Running { .. } => false,
            State::Completed { .. } => true,
        }
    }

    fn do_deferred_work(&mut self, deps: &mut DepsT, deferred: &mut Vec<(usize, DepsT::Output)>) {
        while let Some((index, output)) = deferred.pop() {
            let (tag, entry) = self.evaluations.get_index(index).unwrap();
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
    }

    pub fn evaluate(&mut self, deps: &mut DepsT, handle: DepsT::CompletedHandle, tag: &DepsT::Tag) {
        let index = self.evaluations.get_index_of(tag).unwrap();
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
                handles.push(handle);
            }
            State::Completed { output } => {
                deps.completed(handle, tag, output);
            }
        }
    }

    pub fn receive_completed(&mut self, deps: &mut DepsT, tag: &DepsT::Tag, output: DepsT::Output) {
        let (index, _, entry) = self.evaluations.get_full(tag).unwrap();
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
        evaluations: &IndexMap<DepsT::Tag, Entry<DepsT>>,
        states: &mut [State<DepsT>],
        index: usize,
        tag: &DepsT::Tag,
        entry: &Entry<DepsT>,
        output: DepsT::Output,
        deferred: &mut Vec<(usize, DepsT::Output)>,
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
                    let (out_edge_tag, out_edge_entry) =
                        evaluations.get_index(*out_edge_index).unwrap();
                    Self::start(
                        deps,
                        states,
                        *out_edge_index,
                        out_edge_tag,
                        out_edge_entry,
                        deferred,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, rc::Rc};
    use TestMessage::*;

    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum TestMessage {
        Start(&'static str, Option<&'static str>, Vec<char>),
        Completed(i64, &'static str, char),
    }

    #[derive(Default)]
    struct TestState {
        messages: Vec<TestMessage>,
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
            self.borrow_mut().messages.push(TestMessage::Start(
                tag,
                *partial,
                inputs.into_iter().copied().collect(),
            ));
            StartResult::InProgress
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

    impl Fixture {
        fn new() -> Self {
            Self {
                test_state: Rc::new(RefCell::new(Default::default())),
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
        Fixture::new(),
        |e, _| e.add("a") => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", None, vec![]),
        };
        |e, d| e.evaluate(d, 2, &"a") => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
        |e, d| e.evaluate(d, 3, &"a") => {
            Completed(3, "a", 'a'),
        };
    }

    script_test! {
        adding_multiple_times,
        Fixture::new(),
        |e, _| e.add("a") => {};
        |e, _| e.add("a") => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", None, vec![]),
        };
        |e, _| e.add("a") => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
        |e, _| e.add("a") => {};
        |e, d| e.evaluate(d, 2, &"a") => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        inputs,
        Fixture::new(),
        |e, _| e.add("e") => {};
        |e, _| e.add_with_inputs("d", ["e"]) => {};
        |e, _| e.add("c") => {};
        |e, _| e.add_with_inputs("b", ["d", "c"]) => {};
        |e, _| e.add_with_inputs("a", ["b", "c"]) => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("c", None, vec![]),
            Start("e", None, vec![]),
        };
        |e, d| e.evaluate(d, 2, &"a") => {};
        |e, d| e.evaluate(d, 3, &"b") => {};
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
        update_new_dependency_not_started,
        Fixture::new(),
        |e, _| e.add("a") => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", None, vec![]),
        };
        |e, _| e.add("b") => {};
        |e, d| e.expand(d, &"a", "partial-a-1", ["b"]) => {
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
        update_new_dependency_started,
        Fixture::new(),
        |e, _| e.add("a") => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", None, vec![]),
        };
        |e, _| e.add("b") => {};
        |e, d| e.evaluate(d, 2, &"b") => {
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
        update_new_dependency_completed,
        Fixture::new(),
        |e, _| e.add("a") => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", None, vec![]),
        };
        |e, _| e.add("b") => {};
        |e, d| e.evaluate(d, 2, &"b") => {
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
        update_new_dependency_cycle,
        Fixture::new(),
        |e, _| e.add("a") => {};
        |e, _| e.add_with_inputs("b", ["a"]) => {};
        |e, d| e.evaluate(d, 1, &"b") => {
            Start("a", None, vec![]),
        };
        |e, d| e.expand(d, &"a", "partial-a-1", ["b"]) => {};
    }
}
