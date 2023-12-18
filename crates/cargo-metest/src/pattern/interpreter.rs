use crate::pattern::parser::*;

#[cfg(test)]
use crate::parse_str;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ArtifactKind {
    Library,
    Binary,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Artifact {
    pub kind: ArtifactKind,
    pub name: String,
    pub package: String,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum CaseKind {
    Test,
    Benchmark,
    Example,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Case {
    pub name: String,
    pub kind: CaseKind,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Context {
    pub artifact: Artifact,
    pub case: Option<Case>,
}

impl From<Artifact> for Context {
    fn from(artifact: Artifact) -> Self {
        Self {
            artifact,
            case: None,
        }
    }
}

impl Context {
    fn case(&self) -> Option<&Case> {
        self.case.as_ref()
    }
}

pub fn maybe_not(a: Option<bool>) -> Option<bool> {
    a.map(|v| !v)
}

pub fn maybe_and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a && b),
        (None, Some(true)) => None,
        (None, Some(false)) => Some(false),
        (Some(true), None) => None,
        (Some(false), None) => Some(false),
        (None, None) => None,
    }
}

pub fn maybe_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a || b),
        (None, Some(true)) => Some(true),
        (None, Some(false)) => None,
        (Some(true), None) => Some(true),
        (Some(false), None) => None,
        (None, None) => None,
    }
}

pub fn interpret_simple_selector(s: SimpleSelector, c: &Context) -> Option<bool> {
    use CompoundSelectorName::*;
    use SimpleSelectorName::*;
    Some(match s.name {
        All | Any | True => true,
        None | False => false,
        Library => matches!(c.artifact.kind, ArtifactKind::Library),
        Compound(Binary) => matches!(c.artifact.kind, ArtifactKind::Binary),
        Compound(Benchmark) => matches!(c.case()?.kind, CaseKind::Benchmark),
        Compound(Test) => matches!(c.case()?.kind, CaseKind::Test),
        Compound(Example) => matches!(c.case()?.kind, CaseKind::Example),
        Compound(Name) => unreachable!("should be parser error"),
        Compound(Package) => unreachable!("should be parser error"),
    })
}

fn interpret_matcher(s: &str, matcher: Matcher) -> bool {
    use Matcher::*;
    match matcher {
        Equals(a) => s == a.0,
        Contains(a) => s.contains(&a.0),
        StartsWith(a) => s.starts_with(&a.0),
        EndsWith(a) => s.ends_with(&a.0),
        Matches(a) => a.0.is_match(s),
        Globs(a) => a.0.is_match(s),
    }
}

pub fn interpret_compound_selector(s: CompoundSelector, c: &Context) -> Option<bool> {
    use CompoundSelectorName::*;
    Some(match s.name {
        Name => interpret_matcher(&c.case()?.name, s.matcher),
        Package => interpret_matcher(&c.artifact.package, s.matcher),
        Binary => {
            matches!(&c.artifact.kind, ArtifactKind::Binary)
                && interpret_matcher(&c.artifact.name, s.matcher)
        }
        Benchmark => {
            matches!(&c.case()?.kind, CaseKind::Benchmark)
                && interpret_matcher(&c.case()?.name, s.matcher)
        }
        Example => {
            matches!(&c.case()?.kind, CaseKind::Example)
                && interpret_matcher(&c.case()?.name, s.matcher)
        }
        Test => {
            matches!(&c.case()?.kind, CaseKind::Test)
                && interpret_matcher(&c.case()?.name, s.matcher)
        }
    })
}

fn interpret_not_expression(n: NotExpression, c: &Context) -> Option<bool> {
    use NotExpression::*;
    match n {
        Not(n) => maybe_not(interpret_not_expression(*n, c)),
        Simple(s) => interpret_simple_expression(s, c),
    }
}

fn interpret_and_expression(a: AndExpression, c: &Context) -> Option<bool> {
    use AndExpression::*;
    match a {
        And(n, a) => maybe_and(
            interpret_not_expression(n, c),
            interpret_and_expression(*a, c),
        ),
        Diff(n, a) => maybe_and(
            interpret_not_expression(n, c),
            maybe_not(interpret_and_expression(*a, c)),
        ),
        Not(n) => interpret_not_expression(n, c),
    }
}

fn interpret_or_expression(o: OrExpression, c: &Context) -> Option<bool> {
    use OrExpression::*;
    match o {
        Or(a, o) => maybe_or(
            interpret_and_expression(a, c),
            interpret_or_expression(*o, c),
        ),
        And(a) => interpret_and_expression(a, c),
    }
}

pub fn interpret_simple_expression(s: SimpleExpression, c: &Context) -> Option<bool> {
    use SimpleExpression::*;
    match s {
        Or(o) => interpret_or_expression(*o, c),
        SimpleSelector(s) => interpret_simple_selector(s, c),
        CompoundSelector(s) => interpret_compound_selector(s, c),
    }
}

pub fn interpret_pattern(s: Pattern, c: &Context) -> Option<bool> {
    interpret_or_expression(s.0, c)
}

#[test]
fn simple_expression_simple_selector() {
    use ArtifactKind::*;
    use CaseKind::*;

    fn test_it(s: &str, artifact: ArtifactKind, case: Option<CaseKind>, expected: Option<bool>) {
        let c = Context {
            artifact: Artifact {
                kind: artifact,
                name: "foo.bin".into(),
                package: "foo".into(),
            },
            case: case.map(|kind| Case {
                name: "foo_test".into(),
                kind,
            }),
        };
        let actual = interpret_simple_expression(parse_str!(SimpleExpression, s).unwrap(), &c);
        assert_eq!(actual, expected);
    }

    // for all inputs, these expression evaluate as true
    for w in ["all", "any", "true"] {
        for a in [Library, Binary] {
            for c in [None, Some(Test), Some(Benchmark), Some(Example)] {
                test_it(w, a, c, Some(true));
            }
        }
    }

    // for all inputs, these expression evaluate as false
    for w in ["none", "false"] {
        for a in [Library, Binary] {
            for c in [None, Some(Test), Some(Benchmark), Some(Example)] {
                test_it(w, a, c, Some(false));
            }
        }
    }

    test_it("library", Library, None, Some(true));
    test_it("library", Library, Some(Test), Some(true));
    test_it("library", Binary, None, Some(false));
    test_it("library", Binary, Some(Test), Some(false));

    test_it("binary", Library, None, Some(false));
    test_it("binary", Library, Some(Test), Some(false));
    test_it("binary", Binary, None, Some(true));
    test_it("binary", Binary, Some(Test), Some(true));

    for a in [Library, Binary] {
        test_it("benchmark", a, None, None);
        test_it("benchmark", a, Some(Test), Some(false));
        test_it("benchmark", a, Some(Benchmark), Some(true));
    }

    for a in [Library, Binary] {
        test_it("test", a, None, None);
        test_it("test", a, Some(Benchmark), Some(false));
        test_it("test", a, Some(Test), Some(true));
    }

    for a in [Library, Binary] {
        test_it("example", a, None, None);
        test_it("example", a, Some(Benchmark), Some(false));
        test_it("example", a, Some(Example), Some(true));
    }
}

#[cfg(test)]
fn test_compound_sel(
    s: &str,
    artifact: ArtifactKind,
    name: impl Into<String>,
    expected: Option<bool>,
) {
    let c = Context {
        artifact: Artifact {
            kind: artifact,
            name: name.into(),
            package: "foo".into(),
        },
        case: None,
    };
    let actual = interpret_simple_expression(parse_str!(SimpleExpression, s).unwrap(), &c);
    assert_eq!(actual, expected);
}

#[test]
fn simple_expression_compound_selector_starts_with() {
    use ArtifactKind::*;

    let p = "binary.starts_with(bar)";
    test_compound_sel(p, Binary, "barbaz", Some(true));
    test_compound_sel(p, Binary, "bazbar", Some(false));
}

#[test]
fn simple_expression_compound_selector_ends_with() {
    use ArtifactKind::*;

    let p = "binary.ends_with(bar)";
    test_compound_sel(p, Binary, "bazbar", Some(true));
    test_compound_sel(p, Binary, "barbaz", Some(false));
}

#[test]
fn simple_expression_compound_selector_equals() {
    use ArtifactKind::*;

    let p = "binary.equals(bar)";
    test_compound_sel(p, Binary, "bar", Some(true));
    test_compound_sel(p, Binary, "baz", Some(false));
}

#[test]
fn simple_expression_compound_selector_contains() {
    use ArtifactKind::*;

    let p = "binary.contains(bar)";
    test_compound_sel(p, Binary, "bazbarbin", Some(true));
    test_compound_sel(p, Binary, "bazbin", Some(false));
}

#[test]
fn simple_expression_compound_selector_matches() {
    use ArtifactKind::*;

    let p = "binary.matches(^[a-z]*$)";
    test_compound_sel(p, Binary, "bazbarbin", Some(true));
    test_compound_sel(p, Binary, "baz-bin", Some(false));
}

#[test]
fn simple_expression_compound_selector_globs() {
    use ArtifactKind::*;

    let p = "binary.globs(baz*)";
    test_compound_sel(p, Binary, "bazbarbin", Some(true));
    test_compound_sel(p, Binary, "binbaz", Some(false));
}

#[cfg(test)]
fn test_compound_sel_case(
    s: &str,
    case_kind: CaseKind,
    name: impl Into<String>,
    expected: Option<bool>,
) {
    let c = Context {
        artifact: Artifact {
            kind: ArtifactKind::Library,
            name: "foo_bin".into(),
            package: "foo".into(),
        },
        case: Some(Case {
            name: name.into(),
            kind: case_kind,
        }),
    };
    let actual = interpret_simple_expression(parse_str!(SimpleExpression, s).unwrap(), &c);
    assert_eq!(actual, expected);
}

#[test]
fn simple_expression_compound_selector_name() {
    use CaseKind::*;

    let p = "name.matches(^[a-z]*$)";
    for k in [Test, Benchmark, Example] {
        test_compound_sel_case(p, k, "bazbarbin", Some(true));
        test_compound_sel_case(p, k, "baz-bin", Some(false));
    }
}

#[test]
fn simple_expression_compound_selector_benchmark() {
    use CaseKind::*;

    let p = "benchmark.matches(^[a-z]*$)";
    test_compound_sel_case(p, Benchmark, "bazbarbin", Some(true));
    test_compound_sel_case(p, Benchmark, "baz-bin", Some(false));
    test_compound_sel_case(p, Test, "bazbarbin", Some(false));
}

#[test]
fn simple_expression_compound_selector_example() {
    use CaseKind::*;

    let p = "example.matches(^[a-z]*$)";
    test_compound_sel_case(p, Example, "bazbarbin", Some(true));
    test_compound_sel_case(p, Example, "baz-bin", Some(false));
    test_compound_sel_case(p, Test, "bazbarbin", Some(false));
}

#[test]
fn and_or_not_diff_expressions() {
    fn test_it(s: &str, expected: Option<bool>) {
        let c = Context {
            artifact: Artifact {
                kind: ArtifactKind::Library,
                name: "foo_bin".into(),
                package: "foo".into(),
            },
            case: Some(Case {
                name: "foo_test".into(),
                kind: CaseKind::Test,
            }),
        };
        let actual = interpret_pattern(parse_str!(Pattern, s).unwrap(), &c);
        assert_eq!(actual, expected);
    }

    test_it(
        "(package.equals(foo) || package.equals(bar)) && test.equals(foo_test)",
        Some(true),
    );
    test_it("package.equals(foo) && test.equals(foo_test)", Some(true));
    test_it("package.equals(foo) || test.equals(foo_test)", Some(true));
    test_it("package.equals(foo) || test.equals(bar_test)", Some(true));
    test_it("package.equals(foo) && !test.equals(bar_test)", Some(true));
    test_it("package.equals(foo) - test.equals(bar_test)", Some(true));

    test_it("package.equals(foo) && test.equals(bar_test)", Some(false));
    test_it("package.equals(bar) || test.equals(bar_test)", Some(false));
    test_it("package.equals(bar) || !test.equals(foo_test)", Some(false));
    test_it("package.equals(foo) - test.equals(foo_test)", Some(false));
}

#[test]
fn and_or_not_diff_maybe_expressions() {
    fn test_it(s: &str, expected: Option<bool>) {
        let c = Context {
            artifact: Artifact {
                kind: ArtifactKind::Library,
                name: "foo_bin".into(),
                package: "foo".into(),
            },
            case: None,
        };
        let actual = interpret_pattern(parse_str!(Pattern, s).unwrap(), &c);
        assert_eq!(actual, expected);
    }

    test_it(
        "(package.equals(foo) || package.equals(bar)) && test.equals(foo_test)",
        None,
    );
    test_it("package.equals(foo) && test.equals(foo_test)", None);
    test_it("package.equals(foo) || test.equals(foo_test)", Some(true));
    test_it("package.equals(foo) || test.equals(bar_test)", Some(true));
    test_it("package.equals(foo) && !test.equals(bar_test)", None);
    test_it("package.equals(foo) - test.equals(bar_test)", None);

    test_it("package.equals(foo) && test.equals(bar_test)", None);
    test_it("package.equals(bar) || test.equals(bar_test)", None);
    test_it("package.equals(bar) || !test.equals(foo_test)", None);
    test_it("package.equals(foo) - test.equals(foo_test)", None);
}
