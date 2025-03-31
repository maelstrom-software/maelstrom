use indexmap::IndexMap;
use std::{fmt::Debug, hash::Hash, mem, num::NonZeroUsize};

pub trait Deps {
    type CompletedHandle;
    type Tag: Debug + Eq + Hash;
    type State;
    type Output;

    fn start(&self, tag: &Self::Tag, state: &Self::State, inputs: Vec<&Self::Output>);
    fn completed(&self, handle: Self::CompletedHandle, tag: &Self::Tag, output: &Self::Output);
}

enum EvaluationState<DepsT: Deps> {
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

struct EvaluationEntry<DepsT: Deps> {
    in_edges: Vec<usize>,
    out_edges: Vec<usize>,
    state: DepsT::State,
}

#[derive(Default)]
pub struct Executor<DepsT: Deps> {
    evaluations: IndexMap<DepsT::Tag, EvaluationEntry<DepsT>>,
    states: Vec<EvaluationState<DepsT>>,
}

impl<DepsT: Deps> Executor<DepsT> {
    pub fn add(
        &mut self,
        tag: DepsT::Tag,
        state: DepsT::State,
        inputs: impl IntoIterator<Item = DepsT::Tag>,
    ) {
        if !self.evaluations.contains_key(&tag) {
            let in_edges = inputs
                .into_iter()
                .map(|in_edge_tag| {
                    self.evaluations
                        .get_index_of(&in_edge_tag)
                        .unwrap_or_else(|| {
                            panic!("{tag:?} depends on {in_edge_tag:?}, which hasn't been added")
                        })
                })
                .collect::<Vec<_>>();
            let (index, _) = self.evaluations.insert_full(
                tag,
                EvaluationEntry {
                    in_edges: in_edges.clone(),
                    out_edges: Default::default(),
                    state,
                },
            );
            self.states.push(EvaluationState::NotStarted);
            for in_edge_index in in_edges {
                self.evaluations[in_edge_index].out_edges.push(index);
            }
        }
    }

    fn start(
        deps: &mut DepsT,
        states: &[EvaluationState<DepsT>],
        tag: &DepsT::Tag,
        entry: &EvaluationEntry<DepsT>,
    ) {
        let inputs = entry
            .in_edges
            .iter()
            .map(|in_edge_index| {
                let EvaluationState::Completed { ref output } = states[*in_edge_index] else {
                    panic!("unexpected state");
                };
                output
            })
            .collect();
        deps.start(tag, &entry.state, inputs);
    }

    fn ensure_started_and_get_completed(
        deps: &mut DepsT,
        evaluations: &IndexMap<DepsT::Tag, EvaluationEntry<DepsT>>,
        states: &mut [EvaluationState<DepsT>],
        index: usize,
    ) -> bool {
        let (tag, entry) = evaluations.get_index(index).unwrap();
        match states[index] {
            EvaluationState::NotStarted => {
                let lacking = entry
                    .in_edges
                    .iter()
                    .filter(|in_edge_index| {
                        !Self::ensure_started_and_get_completed(
                            deps,
                            evaluations,
                            states,
                            **in_edge_index,
                        )
                    })
                    .count();
                let handles = Default::default();
                match NonZeroUsize::new(lacking) {
                    Some(lacking) => {
                        states[index] = EvaluationState::WaitingOnInputs { lacking, handles };
                    }
                    None => {
                        states[index] = EvaluationState::Running { handles };
                        Self::start(deps, states, tag, entry);
                    }
                }
                false
            }
            EvaluationState::WaitingOnInputs { .. } | EvaluationState::Running { .. } => false,
            EvaluationState::Completed { .. } => true,
        }
    }

    pub fn evaluate(&mut self, deps: &mut DepsT, handle: DepsT::CompletedHandle, tag: &DepsT::Tag) {
        let index = self.evaluations.get_index_of(tag).unwrap();
        Self::ensure_started_and_get_completed(deps, &self.evaluations, &mut self.states, index);
        match &mut self.states[index] {
            EvaluationState::NotStarted => {
                panic!("unexpected state");
            }
            EvaluationState::WaitingOnInputs { handles, .. }
            | EvaluationState::Running { handles } => {
                handles.push(handle);
            }
            EvaluationState::Completed { output } => {
                deps.completed(handle, tag, output);
            }
        }
    }

    pub fn receive_completed(&mut self, deps: &mut DepsT, tag: &DepsT::Tag, output: DepsT::Output) {
        let (index, _, entry) = self.evaluations.get_full(tag).unwrap();
        let state = &mut self.states[index];
        let EvaluationState::Running { handles: waiting } =
            mem::replace(state, EvaluationState::Completed { output })
        else {
            panic!("unexpected state");
        };
        let EvaluationState::Completed { output } = state else {
            panic!("unexpected state");
        };
        for handle in waiting {
            deps.completed(handle, tag, output);
        }

        for out_edge_index in &entry.out_edges {
            let out_edge_state = &mut self.states[*out_edge_index];
            let EvaluationState::WaitingOnInputs { handles, lacking } = out_edge_state else {
                continue;
            };
            match NonZeroUsize::new(lacking.get() - 1) {
                Some(new_lacking) => {
                    *lacking = new_lacking;
                }
                None => {
                    *out_edge_state = EvaluationState::Running {
                        handles: mem::take(handles),
                    };
                    let (out_edge_tag, out_edge_entry) =
                        self.evaluations.get_index(*out_edge_index).unwrap();
                    Self::start(deps, &self.states, out_edge_tag, out_edge_entry);
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
        Start(&'static str, &'static str, Vec<char>),
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
        type State = &'static str;

        fn start(&self, tag: &Self::Tag, state: &Self::State, inputs: Vec<&Self::Output>) {
            self.borrow_mut().messages.push(TestMessage::Start(
                tag,
                state,
                inputs.into_iter().copied().collect(),
            ));
        }

        fn completed(&self, handle: Self::CompletedHandle, tag: &Self::Tag, output: &Self::Output) {
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
        |e, _| e.add("a", "a-state", []) => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", "a-state", vec![]),
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
        |e, _| e.add("a", "a-state", []) => {};
        |e, _| e.add("a", "a-state-2", []) => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("a", "a-state", vec![]),
        };
        |e, _| e.add("a", "a-state-3", []) => {};
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
        };
        |e, _| e.add("a", "a-state-4", []) => {};
        |e, d| e.evaluate(d, 2, &"a") => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        inputs,
        Fixture::new(),
        |e, _| e.add("e", "e-state", []) => {};
        |e, _| e.add("d", "d-state", ["e"]) => {};
        |e, _| e.add("c", "c-state", []) => {};
        |e, _| e.add("b", "b-state", ["d", "c"]) => {};
        |e, _| e.add("a", "a-state", ["b", "c"]) => {};
        |e, d| e.evaluate(d, 1, &"a") => {
            Start("c", "c-state", vec![]),
            Start("e", "e-state", vec![]),
        };
        |e, d| e.evaluate(d, 2, &"a") => {};
        |e, d| e.evaluate(d, 3, &"b") => {};
        |e, d| e.receive_completed(d, &"e", 'e') => {
            Start("d", "d-state", vec!['e']),
        };
        |e, d| e.receive_completed(d, &"c", 'c') => {};
        |e, d| e.receive_completed(d, &"d", 'd') => {
            Start("b", "b-state", vec!['d', 'c']),
        };
        |e, d| e.receive_completed(d, &"b", 'b') => {
            Start("a", "a-state", vec!['b', 'c']),
            Completed(3, "b", 'b'),
        };
        |e, d| e.receive_completed(d, &"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
    }
}
