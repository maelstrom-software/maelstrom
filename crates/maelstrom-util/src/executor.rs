use indexmap::IndexMap;
use std::{fmt::Debug, hash::Hash, mem, num::NonZeroUsize};

pub trait Deps {
    type CompletedHandle;
    type Tag: Debug + Eq + Hash;
    type Output;

    fn start(&self, tag: &Self::Tag, inputs: Vec<&Self::Output>);
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

struct EvaluationEntry {
    in_edges: Vec<usize>,
    out_edges: Vec<usize>,
}

pub struct Executor<DepsT: Deps> {
    deps: DepsT,
    evaluations: IndexMap<DepsT::Tag, EvaluationEntry>,
    states: Vec<EvaluationState<DepsT>>,
}

impl<DepsT: Deps> Executor<DepsT> {
    pub fn new(deps: DepsT) -> Self {
        Self {
            deps,
            evaluations: Default::default(),
            states: Default::default(),
        }
    }

    pub fn add(&mut self, tag: DepsT::Tag, inputs: impl IntoIterator<Item = DepsT::Tag>) {
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
        entry: &EvaluationEntry,
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
        deps.start(tag, inputs);
    }

    fn ensure_started_and_get_completed(
        deps: &mut DepsT,
        evaluations: &IndexMap<DepsT::Tag, EvaluationEntry>,
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

    pub fn evaluate(&mut self, handle: DepsT::CompletedHandle, tag: &DepsT::Tag) {
        let index = self.evaluations.get_index_of(tag).unwrap();
        Self::ensure_started_and_get_completed(
            &mut self.deps,
            &self.evaluations,
            &mut self.states,
            index,
        );
        match &mut self.states[index] {
            EvaluationState::NotStarted => {
                panic!("unexpected state");
            }
            EvaluationState::WaitingOnInputs { handles, .. }
            | EvaluationState::Running { handles } => {
                handles.push(handle);
            }
            EvaluationState::Completed { output } => {
                self.deps.completed(handle, tag, output);
            }
        }
    }

    pub fn receive_completed(&mut self, tag: &DepsT::Tag, output: DepsT::Output) {
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
            self.deps.completed(handle, tag, output);
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
                    Self::start(&mut self.deps, &self.states, out_edge_tag, out_edge_entry);
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
        Start(&'static str, Vec<char>),
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

        fn start(&self, tag: &Self::Tag, inputs: Vec<&Self::Output>) {
            self.borrow_mut().messages.push(TestMessage::Start(
                tag,
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
            let test_state = Rc::new(RefCell::new(Default::default()));
            let sut = Executor::new(test_state.clone());
            Self { test_state, sut }
        }

        fn call_method(&mut self, method: impl FnOnce(&mut Executor<Rc<RefCell<TestState>>>)) {
            method(&mut self.sut);
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
        |e| e.add("a", []) => {};
        |e| e.evaluate(1, &"a") => {
            Start("a", vec![]),
        };
        |e| e.evaluate(2, &"a") => {};
        |e| e.receive_completed(&"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
        |e| e.evaluate(3, &"a") => {
            Completed(3, "a", 'a'),
        };
    }

    script_test! {
        adding_multiple_times,
        Fixture::new(),
        |e| e.add("a", []) => {};
        |e| e.add("a", []) => {};
        |e| e.evaluate(1, &"a") => {
            Start("a", vec![]),
        };
        |e| e.add("a", []) => {};
        |e| e.receive_completed(&"a", 'a') => {
            Completed(1, "a", 'a'),
        };
        |e| e.add("a", []) => {};
        |e| e.evaluate(2, &"a") => {
            Completed(2, "a", 'a'),
        };
    }

    script_test! {
        inputs,
        Fixture::new(),
        |e| e.add("e", []) => {};
        |e| e.add("d", ["e"]) => {};
        |e| e.add("c", []) => {};
        |e| e.add("b", ["d", "c"]) => {};
        |e| e.add("a", ["b", "c"]) => {};
        |e| e.evaluate(1, &"a") => {
            Start("c", vec![]),
            Start("e", vec![]),
        };
        |e| e.evaluate(2, &"a") => {};
        |e| e.evaluate(3, &"b") => {};
        |e| e.receive_completed(&"e", 'e') => {
            Start("d", vec!['e']),
        };
        |e| e.receive_completed(&"c", 'c') => {};
        |e| e.receive_completed(&"d", 'd') => {
            Start("b", vec!['d', 'c']),
        };
        |e| e.receive_completed(&"b", 'b') => {
            Start("a", vec!['b', 'c']),
            Completed(3, "b", 'b'),
        };
        |e| e.receive_completed(&"a", 'a') => {
            Completed(1, "a", 'a'),
            Completed(2, "a", 'a'),
        };
    }
}
