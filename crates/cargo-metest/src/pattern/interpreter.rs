use crate::pattern::parser::*;

#[cfg(test)]
use crate::parse_str;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ArtifactKind {
    Library,
    Binary,
    Test,
    Benchmark,
    Example,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Artifact {
    pub kind: ArtifactKind,
    pub name: String,
    pub package: String,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Case {
    pub name: String,
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

pub fn interpret_simple_selector(s: &SimpleSelector, c: &Context) -> bool {
    use CompoundSelectorName::*;
    use SimpleSelectorName::*;
    match s.name {
        All | Any | True => true,
        None | False => false,
        Library => matches!(c.artifact.kind, ArtifactKind::Library),
        Compound(Binary) => matches!(c.artifact.kind, ArtifactKind::Binary),
        Compound(Benchmark) => matches!(c.artifact.kind, ArtifactKind::Benchmark),
        Compound(Test) => matches!(c.artifact.kind, ArtifactKind::Test),
        Compound(Example) => matches!(c.artifact.kind, ArtifactKind::Example),
        Compound(Name) => unreachable!("should be parser error"),
        Compound(Package) => unreachable!("should be parser error"),
    }
}

fn interpret_matcher(s: &str, matcher: &Matcher) -> bool {
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

pub fn interpret_compound_selector(s: &CompoundSelector, c: &Context) -> Option<bool> {
    use CompoundSelectorName::*;
    Some(match s.name {
        Name => interpret_matcher(&c.case()?.name, &s.matcher),
        Package => interpret_matcher(&c.artifact.package, &s.matcher),
        Binary => {
            matches!(&c.artifact.kind, ArtifactKind::Binary)
                && interpret_matcher(&c.artifact.name, &s.matcher)
        }
        Benchmark => {
            matches!(&c.artifact.kind, ArtifactKind::Benchmark)
                && interpret_matcher(&c.artifact.name, &s.matcher)
        }
        Example => {
            matches!(&c.artifact.kind, ArtifactKind::Example)
                && interpret_matcher(&c.artifact.name, &s.matcher)
        }
        Test => {
            matches!(&c.artifact.kind, ArtifactKind::Test)
                && interpret_matcher(&c.artifact.name, &s.matcher)
        }
    })
}

fn interpret_not_expression(n: &NotExpression, c: &Context) -> Option<bool> {
    use NotExpression::*;
    match n {
        Not(n) => maybe_not(interpret_not_expression(&*n, c)),
        Simple(s) => interpret_simple_expression(s, c),
    }
}

fn interpret_and_expression(a: &AndExpression, c: &Context) -> Option<bool> {
    use AndExpression::*;
    match a {
        And(n, a) => maybe_and(
            interpret_not_expression(n, c),
            interpret_and_expression(&*a, c),
        ),
        Diff(n, a) => maybe_and(
            interpret_not_expression(n, c),
            maybe_not(interpret_and_expression(&*a, c)),
        ),
        Not(n) => interpret_not_expression(n, c),
    }
}

fn interpret_or_expression(o: &OrExpression, c: &Context) -> Option<bool> {
    use OrExpression::*;
    match o {
        Or(a, o) => maybe_or(
            interpret_and_expression(a, c),
            interpret_or_expression(&*o, c),
        ),
        And(a) => interpret_and_expression(a, c),
    }
}

pub fn interpret_simple_expression(s: &SimpleExpression, c: &Context) -> Option<bool> {
    use SimpleExpression::*;
    match s {
        Or(o) => interpret_or_expression(&*o, c),
        SimpleSelector(s) => Some(interpret_simple_selector(s, c)),
        CompoundSelector(s) => interpret_compound_selector(s, c),
    }
}

pub fn interpret_pattern(s: &Pattern, c: &Context) -> Option<bool> {
    interpret_or_expression(&s.0, c)
}

#[test]
fn simple_expression_simple_selector() {
    use ArtifactKind::*;

    fn test_it(s: &str, artifact: ArtifactKind, expected: bool) {
        let c = Context {
            artifact: Artifact {
                kind: artifact,
                name: "foo.bin".into(),
                package: "foo".into(),
            },
            case: None,
        };
        let actual = interpret_simple_expression(&parse_str!(SimpleExpression, s).unwrap(), &c);
        assert_eq!(actual, Some(expected));
    }

    // for all inputs, these expression evaluate as true
    for w in ["all", "any", "true"] {
        for a in [Library, Binary, Test, Benchmark, Example] {
            test_it(w, a, true);
        }
    }

    // for all inputs, these expression evaluate as false
    for w in ["none", "false"] {
        for a in [Library, Binary, Test, Benchmark, Example] {
            test_it(w, a, false);
        }
    }

    test_it("library", Library, true);
    test_it("library", Binary, false);

    test_it("binary", Library, false);
    test_it("binary", Binary, true);

    test_it("benchmark", Library, false);
    test_it("benchmark", Benchmark, true);

    test_it("test", Library, false);
    test_it("test", Test, true);

    test_it("example", Library, false);
    test_it("example", Example, true);
}

#[cfg(test)]
fn test_compound_sel(s: &str, artifact: ArtifactKind, name: impl Into<String>, expected: bool) {
    let c = Context {
        artifact: Artifact {
            kind: artifact,
            name: name.into(),
            package: "foo".into(),
        },
        case: None,
    };
    let actual = interpret_simple_expression(&parse_str!(SimpleExpression, s).unwrap(), &c);
    assert_eq!(actual, Some(expected));
}

#[test]
fn simple_expression_compound_selector_starts_with() {
    use ArtifactKind::*;

    let p = "binary.starts_with(bar)";
    test_compound_sel(p, Binary, "barbaz", true);
    test_compound_sel(p, Binary, "bazbar", false);
}

#[test]
fn simple_expression_compound_selector_ends_with() {
    use ArtifactKind::*;

    let p = "binary.ends_with(bar)";
    test_compound_sel(p, Binary, "bazbar", true);
    test_compound_sel(p, Binary, "barbaz", false);
}

#[test]
fn simple_expression_compound_selector_equals() {
    use ArtifactKind::*;

    let p = "binary.equals(bar)";
    test_compound_sel(p, Binary, "bar", true);
    test_compound_sel(p, Binary, "baz", false);
}

#[test]
fn simple_expression_compound_selector_contains() {
    use ArtifactKind::*;

    let p = "binary.contains(bar)";
    test_compound_sel(p, Binary, "bazbarbin", true);
    test_compound_sel(p, Binary, "bazbin", false);
}

#[test]
fn simple_expression_compound_selector_matches() {
    use ArtifactKind::*;

    let p = "binary.matches(^[a-z]*$)";
    test_compound_sel(p, Binary, "bazbarbin", true);
    test_compound_sel(p, Binary, "baz-bin", false);
}

#[test]
fn simple_expression_compound_selector_globs() {
    use ArtifactKind::*;

    let p = "binary.globs(baz*)";
    test_compound_sel(p, Binary, "bazbarbin", true);
    test_compound_sel(p, Binary, "binbaz", false);
}

#[cfg(test)]
fn test_compound_sel_case(
    s: &str,
    kind: ArtifactKind,
    artifact_name: impl Into<String>,
    case_name: impl Into<String>,
    expected: bool,
) {
    let c = Context {
        artifact: Artifact {
            kind,
            name: artifact_name.into(),
            package: "foo".into(),
        },
        case: Some(Case {
            name: case_name.into(),
        }),
    };
    let actual = interpret_simple_expression(&parse_str!(SimpleExpression, s).unwrap(), &c);
    assert_eq!(actual, Some(expected));
}

#[test]
fn simple_expression_compound_selector_name() {
    use ArtifactKind::*;

    let p = "name.matches(^[a-z]*$)";
    for k in [Library, Binary, Test, Benchmark, Example] {
        test_compound_sel_case(p, k, "", "bazbarbin", true);
        test_compound_sel_case(p, k, "", "baz-bin", false);
    }
}

#[test]
fn simple_expression_compound_selector_binary() {
    use ArtifactKind::*;

    let p = "binary.matches(^[a-z]*$)";
    test_compound_sel_case(p, Binary, "bazbarbin", "", true);
    test_compound_sel_case(p, Binary, "baz-bin", "", false);
    test_compound_sel_case(p, Test, "bazbarbin", "", false);
}

#[test]
fn simple_expression_compound_selector_benchmark() {
    use ArtifactKind::*;

    let p = "benchmark.matches(^[a-z]*$)";
    test_compound_sel_case(p, Benchmark, "bazbarbin", "", true);
    test_compound_sel_case(p, Benchmark, "baz-bin", "", false);
    test_compound_sel_case(p, Test, "bazbarbin", "", false);
}

#[test]
fn simple_expression_compound_selector_example() {
    use ArtifactKind::*;

    let p = "example.matches(^[a-z]*$)";
    test_compound_sel_case(p, Example, "bazbarbin", "", true);
    test_compound_sel_case(p, Example, "baz-bin", "", false);
    test_compound_sel_case(p, Test, "bazbarbin", "", false);
}

#[test]
fn simple_expression_compound_selector_test() {
    use ArtifactKind::*;

    let p = "test.matches(^[a-z]*$)";
    test_compound_sel_case(p, Test, "bazbarbin", "", true);
    test_compound_sel_case(p, Test, "baz-bin", "", false);
    test_compound_sel_case(p, Binary, "bazbarbin", "", false);
}

#[test]
fn and_or_not_diff_expressions() {
    fn test_it(s: &str, expected: bool) {
        let c = Context {
            artifact: Artifact {
                kind: ArtifactKind::Library,
                name: "foo_bin".into(),
                package: "foo".into(),
            },
            case: Some(Case {
                name: "foo_test".into(),
            }),
        };
        let actual = interpret_pattern(&parse_str!(Pattern, s).unwrap(), &c);
        assert_eq!(actual, Some(expected));
    }

    test_it(
        "(package.equals(foo) || package.equals(bar)) && name.equals(foo_test)",
        true,
    );
    test_it("package.equals(foo) && name.equals(foo_test)", true);
    test_it("package.equals(foo) || name.equals(foo_test)", true);
    test_it("package.equals(foo) || name.equals(bar_test)", true);
    test_it("package.equals(foo) && !name.equals(bar_test)", true);
    test_it("package.equals(foo) - name.equals(bar_test)", true);

    test_it("package.equals(foo) && name.equals(bar_test)", false);
    test_it("package.equals(bar) || name.equals(bar_test)", false);
    test_it("package.equals(bar) || !name.equals(foo_test)", false);
    test_it("package.equals(foo) - name.equals(foo_test)", false);
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
        let actual = interpret_pattern(&parse_str!(Pattern, s).unwrap(), &c);
        assert_eq!(actual, expected);
    }

    test_it(
        "(package.equals(foo) || package.equals(bar)) && name.equals(foo_test)",
        None,
    );
    test_it("package.equals(foo) && name.equals(foo_test)", None);
    test_it("name.equals(foo_test) && name.equals(bar_test)", None);
    test_it("name.equals(foo_test) && package.equals(foo)", None);
    test_it("package.equals(foo) && name.equals(bar_test)", None);
    test_it("package.equals(foo) && !name.equals(bar_test)", None);

    test_it("name.equals(foo_test) && package.equals(bar)", Some(false));
    test_it("package.equals(bar) && name.equals(foo_test)", Some(false));

    test_it("name.equals(foo_test) || name.equals(bar_test)", None);
    test_it("name.equals(foo_test) || package.equals(bar)", None);
    test_it("package.equals(bar) || name.equals(bar_test)", None);
    test_it("package.equals(bar) || !name.equals(foo_test)", None);

    test_it("name.equals(foo_test) || package.equals(foo)", Some(true));
    test_it("package.equals(foo) || name.equals(foo_test)", Some(true));
    test_it("package.equals(foo) || name.equals(bar_test)", Some(true));

    test_it("package.equals(foo) - name.equals(bar_test)", None);
    test_it("package.equals(foo) - name.equals(foo_test)", None);
}
