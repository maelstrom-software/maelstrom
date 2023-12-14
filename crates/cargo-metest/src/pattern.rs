use combine::{
    attempt, between, choice, many, many1, optional, parser,
    parser::{
        char::{space, spaces, string},
        combinator::{lazy, no_partial},
    },
    satisfy, token, Parser, Stream,
};
use derive_more::From;

#[cfg(test)]
use crate::parse_str;

#[derive(From, Debug, PartialEq)]
#[from(forward)]
pub struct MatcherParameter(pub String);

impl MatcherParameter {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        parser(|input| {
            let (open, committed) =
                choice((token('('), token('['), token('{'), token('<'), token('/')))
                    .parse_stream(input)
                    .into_result()?;
            let close = match open {
                '(' => ')',
                '[' => ']',
                '{' => '}',
                '<' => '>',
                '/' => '/',
                _ => unreachable!(),
            };
            let mut count = 1;
            let mut contents = String::new();
            'outer: loop {
                let (chunk, _): (String, _) = many(satisfy(|c| c != open && c != close))
                    .parse_stream(input)
                    .into_result()?;
                contents += &chunk;

                while attempt(token(close)).parse_stream(input).is_ok() {
                    count -= 1;
                    if count == 0 {
                        break 'outer;
                    } else {
                        contents.push(close);
                    }
                }
                count += 1;
                token(open).parse_stream(input).into_result()?;
                contents.push(open);
            }

            Ok((contents, committed))
        })
        .map(Self)
    }
}

#[test]
fn matcher_parameter_test() {
    fn test_it(a: &str, b: &str) {
        assert_eq!(
            parse_str!(MatcherParameter, a),
            Ok(MatcherParameter(b.into()))
        );
    }
    test_it("[abc]", "abc");
    test_it("{abc}", "abc");
    test_it("<abc>", "abc");
    test_it("[(hello)]", "(hello)");
    test_it("((hello))", "(hello)");
    test_it("(([hello]))", "([hello])");
    test_it("(he[llo)", "he[llo");
    test_it("()", "");
    test_it("((()))", "(())");
    test_it("((a)(b))", "(a)(b)");

    fn test_err(a: &str) {
        assert!(matches!(parse_str!(MatcherParameter, a), Err(_)));
    }
    test_err("[1)");
    test_err("(((hello))");
}

#[derive(Debug, PartialEq)]
pub enum MatcherName {
    Equals,
    Contains,
    StartsWith,
    EndsWith,
    Matches,
    Globs,
}

impl MatcherName {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        choice((
            attempt(string("equals")).map(|_| Self::Equals),
            attempt(string("contains")).map(|_| Self::Contains),
            attempt(string("starts_with")).map(|_| Self::StartsWith),
            attempt(string("ends_with")).map(|_| Self::EndsWith),
            attempt(string("matches")).map(|_| Self::Matches),
            string("globs").map(|_| Self::Globs),
        ))
    }
}

#[derive(Debug, PartialEq)]
pub enum CompoundSelectorName {
    Name,
    Binary,
    Benchmark,
    Example,
    Test,
}

impl CompoundSelectorName {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        choice((
            attempt(string("name")).map(|_| Self::Name),
            attempt(string("binary")).map(|_| Self::Binary),
            attempt(string("benchmark")).map(|_| Self::Benchmark),
            attempt(string("example")).map(|_| Self::Example),
            string("test").map(|_| Self::Test),
        ))
    }
}

#[derive(Debug, PartialEq)]
pub struct CompoundSelector {
    pub name: CompoundSelectorName,
    pub matcher_name: MatcherName,
    pub matcher_parameter: MatcherParameter,
}

impl CompoundSelector {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        (
            CompoundSelectorName::parser().skip(token('.')),
            MatcherName::parser(),
            MatcherParameter::parser(),
        )
            .map(|(name, matcher_name, matcher_parameter)| Self {
                name,
                matcher_name,
                matcher_parameter,
            })
    }
}

#[derive(Debug, PartialEq, From)]
pub enum SimpleSelectorName {
    All,
    Any,
    True,
    None,
    False,
    Library,
    #[from]
    Compound(CompoundSelectorName),
}

impl SimpleSelectorName {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        choice((
            attempt(string("all")).map(|_| Self::All),
            attempt(string("any")).map(|_| Self::Any),
            attempt(string("true")).map(|_| Self::True),
            attempt(string("none")).map(|_| Self::None),
            attempt(string("false")).map(|_| Self::False),
            attempt(string("library")).map(|_| Self::Library),
            CompoundSelectorName::parser().map(Self::Compound),
        ))
    }
}

#[derive(Debug, PartialEq, From)]
#[from(types(CompoundSelectorName))]
pub struct SimpleSelector {
    pub name: SimpleSelectorName,
}

impl SimpleSelector {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        SimpleSelectorName::parser()
            .skip(optional(string("()")))
            .map(|name| Self { name })
    }
}

#[derive(Debug, PartialEq, From)]
pub enum SimpleExpression {
    #[from(types(OrExpression))]
    Or(Box<OrExpression>),
    #[from(types(SimpleSelectorName, CompoundSelectorName))]
    SimpleSelector(SimpleSelector),
    #[from]
    CompoundSelector(CompoundSelector),
}

impl From<AndExpression> for SimpleExpression {
    fn from(a: AndExpression) -> Self {
        OrExpression::from(a).into()
    }
}

impl SimpleExpression {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        let or_parser = || no_partial(lazy(|| OrExpression::parser())).boxed();
        choice((
            attempt(between(
                token('(').skip(spaces()),
                spaces().with(token(')')),
                or_parser(),
            ))
            .map(|o| Self::Or(Box::new(o))),
            attempt(CompoundSelector::parser().map(Self::CompoundSelector)),
            attempt(SimpleSelector::parser().map(Self::SimpleSelector)),
        ))
    }
}

fn not_operator<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = &'static str> {
    choice((string("!"), string("~"), string("not").skip(spaces1())))
}

#[derive(Debug, PartialEq, From)]
pub enum NotExpression {
    Not(Box<NotExpression>),
    #[from(types(SimpleSelector, SimpleSelectorName, CompoundSelector, OrExpression))]
    Simple(SimpleExpression),
}

impl NotExpression {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        let self_parser = || no_partial(lazy(|| Self::parser())).boxed();
        choice((
            attempt(not_operator().with(self_parser().map(|e| Self::Not(Box::new(e))))),
            SimpleExpression::parser().map(Self::Simple),
        ))
    }
}

fn spaces1<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = String> {
    many1(space())
}

fn and_operator<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = &'static str> {
    attempt(between(
        spaces(),
        spaces(),
        choice((attempt(string("&&")), string("&"), string("+"))),
    ))
    .or(spaces1().with(string("and")).skip(spaces1()))
}

fn diff_operator<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = &'static str> {
    attempt(between(
        spaces(),
        spaces(),
        choice((string("\\"), string("-"))),
    ))
    .or(spaces1().with(string("minus")).skip(spaces1()))
}

#[derive(Debug, PartialEq, From)]
pub enum AndExpression {
    And(NotExpression, Box<AndExpression>),
    Diff(NotExpression, Box<AndExpression>),
    #[from(types(SimpleExpression, SimpleSelector, SimpleSelectorName, CompoundSelector))]
    Not(NotExpression),
}

impl AndExpression {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        let self_parser = || no_partial(lazy(|| Self::parser())).boxed();
        choice((
            attempt((NotExpression::parser(), and_operator(), self_parser()))
                .map(|(n, _, a)| Self::And(n, Box::new(a))),
            attempt((NotExpression::parser(), diff_operator(), self_parser()))
                .map(|(n, _, a)| Self::Diff(n, Box::new(a))),
            NotExpression::parser().map(Self::Not),
        ))
    }
}

fn or_operator<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = &'static str> {
    attempt(between(
        spaces(),
        spaces(),
        choice((attempt(string("||")), string("|"))),
    ))
    .or(spaces1().with(string("or")).skip(spaces1()))
}

#[derive(Debug, PartialEq, From)]
pub enum OrExpression {
    Or(AndExpression, Box<OrExpression>),
    #[from(types(
        NotExpression,
        SimpleExpression,
        SimpleSelector,
        SimpleSelectorName,
        CompoundSelector
    ))]
    And(AndExpression),
}

impl OrExpression {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        let self_parser = || no_partial(lazy(|| Self::parser())).boxed();
        choice((
            attempt((AndExpression::parser(), or_operator(), self_parser()))
                .map(|(a, _, o)| Self::Or(a, Box::new(o))),
            AndExpression::parser().map(Self::And),
        ))
    }
}

#[derive(Debug, PartialEq, From)]
#[from(types(NotExpression, AndExpression))]
pub struct Pattern(OrExpression);

impl Pattern {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        OrExpression::parser().map(Self)
    }
}

#[macro_export]
macro_rules! parse_str {
    ($ty:ty, $input:expr) => {{
        use combine::{EasyParser as _, Parser as _};
        <$ty>::parser()
            .skip(combine::eof())
            .easy_parse(combine::stream::position::Stream::new($input))
            .map(|x| x.0)
    }};
}

#[test]
fn simple_expr() {
    use CompoundSelectorName::*;
    use SimpleSelectorName::*;

    fn test_it(a: &str, s: impl Into<SimpleExpression>) {
        assert_eq!(parse_str!(SimpleExpression, a), Ok(s.into()));
    }
    test_it("all", All);
    test_it("all()", All);
    test_it("any", Any);
    test_it("any()", Any);
    test_it("true", True);
    test_it("true()", True);
    test_it("none", None);
    test_it("none()", None);
    test_it("false", False);
    test_it("false()", False);
    test_it("library", Library);
    test_it("library()", Library);

    test_it("name", Name);
    test_it("name()", Name);
    test_it("binary", Binary);
    test_it("binary()", Binary);
    test_it("benchmark", Benchmark);
    test_it("benchmark()", Benchmark);
    test_it("example", Example);
    test_it("example()", Example);
    test_it("test", Test);
    test_it("test()", Test);
}

#[test]
fn simple_expr_compound() {
    use CompoundSelectorName::*;
    use MatcherName::*;

    fn test_it(
        a: &str,
        name: CompoundSelectorName,
        matcher_name: MatcherName,
        matcher_parameter: impl Into<MatcherParameter>,
    ) {
        assert_eq!(
            parse_str!(SimpleExpression, a),
            Ok(CompoundSelector {
                name,
                matcher_name,
                matcher_parameter: matcher_parameter.into()
            }
            .into())
        );
    }
    test_it("name.matches<foo>", Name, Matches, "foo");
    test_it("test.equals([a-z].*)", Test, Equals, "[a-z].*");
    test_it("binary.starts_with<(hi)>", Binary, StartsWith, "(hi)");
    test_it("benchmark.ends_with[hey?]", Benchmark, EndsWith, "hey?");
    test_it("example.contains{s(oi)l}", Example, Contains, "s(oi)l");
}

#[test]
fn pattern_simple_boolean_expr() {
    fn test_it(a: &str, pattern: impl Into<Pattern>) {
        assert_eq!(parse_str!(Pattern, a), Ok(pattern.into()));
    }
    test_it(
        "!all",
        NotExpression::Not(Box::new(SimpleSelectorName::All.into())),
    );
    test_it(
        "all && any",
        AndExpression::And(
            SimpleSelectorName::All.into(),
            Box::new(SimpleSelectorName::Any.into()),
        ),
    );
    test_it(
        "all || any",
        OrExpression::Or(
            SimpleSelectorName::All.into(),
            Box::new(SimpleSelectorName::Any.into()),
        ),
    );
}

#[test]
fn pattern_longer_boolean_expr() {
    fn test_it(a: &str, pattern: impl Into<Pattern>) {
        assert_eq!(parse_str!(Pattern, a), Ok(pattern.into()));
    }
    test_it(
        "all || any || none",
        OrExpression::Or(
            SimpleSelectorName::All.into(),
            Box::new(
                OrExpression::Or(
                    SimpleSelectorName::Any.into(),
                    Box::new(SimpleSelectorName::None.into()),
                )
                .into(),
            ),
        ),
    );
    test_it(
        "all || any && none",
        OrExpression::Or(
            SimpleSelectorName::All.into(),
            Box::new(
                AndExpression::And(
                    SimpleSelectorName::Any.into(),
                    Box::new(SimpleSelectorName::None.into()),
                )
                .into(),
            ),
        ),
    );
    test_it(
        "all && any || none",
        OrExpression::Or(
            AndExpression::And(
                SimpleSelectorName::All.into(),
                Box::new(SimpleSelectorName::Any.into()),
            ),
            Box::new(SimpleSelectorName::None.into()),
        ),
    );
}

#[test]
fn pattern_complicated_boolean_expr() {
    fn test_it(a: &str, pattern: impl Into<Pattern>) {
        assert_eq!(parse_str!(Pattern, a), Ok(pattern.into()));
    }
    test_it(
        "( all || any ) && none - library",
        AndExpression::And(
            OrExpression::Or(
                SimpleSelectorName::All.into(),
                Box::new(SimpleSelectorName::Any.into()),
            )
            .into(),
            Box::new(AndExpression::Diff(
                SimpleSelectorName::None.into(),
                Box::new(SimpleSelectorName::Library.into()),
            )),
        ),
    );
    test_it(
        "!( all || any ) && none",
        AndExpression::And(
            NotExpression::Not(Box::new(
                OrExpression::Or(
                    SimpleSelectorName::All.into(),
                    Box::new(SimpleSelectorName::Any.into()),
                )
                .into(),
            )),
            Box::new(SimpleSelectorName::None.into()),
        ),
    );

    test_it(
        "not ( all or any ) and none minus library",
        AndExpression::And(
            NotExpression::Not(Box::new(
                OrExpression::Or(
                    SimpleSelectorName::All.into(),
                    Box::new(SimpleSelectorName::Any.into()),
                )
                .into(),
            )),
            Box::new(AndExpression::Diff(
                SimpleSelectorName::None.into(),
                Box::new(SimpleSelectorName::Library.into()),
            )),
        ),
    );
}

#[test]
fn pattern_complicated_boolean_expr_compound() {
    fn test_it(a: &str, pattern: impl Into<Pattern>) {
        assert_eq!(parse_str!(Pattern, a), Ok(pattern.into()));
    }

    test_it(
        "binary.starts_with(hi) && name.matches/([a-z]+::)*[a-z]+/",
        AndExpression::And(
            CompoundSelector {
                name: CompoundSelectorName::Binary,
                matcher_name: MatcherName::StartsWith,
                matcher_parameter: "hi".into(),
            }
            .into(),
            Box::new(
                CompoundSelector {
                    name: CompoundSelectorName::Name,
                    matcher_name: MatcherName::Matches,
                    matcher_parameter: "([a-z]+::)*[a-z]+".into(),
                }
                .into(),
            ),
        ),
    );

    test_it(
        "( binary.starts_with(hi) && name.matches/([a-z]+::)*[a-z]+/ ) || benchmark.ends_with(jo)",
        OrExpression::Or(
            NotExpression::Simple(
                AndExpression::And(
                    CompoundSelector {
                        name: CompoundSelectorName::Binary,
                        matcher_name: MatcherName::StartsWith,
                        matcher_parameter: "hi".into(),
                    }
                    .into(),
                    Box::new(
                        CompoundSelector {
                            name: CompoundSelectorName::Name,
                            matcher_name: MatcherName::Matches,
                            matcher_parameter: "([a-z]+::)*[a-z]+".into(),
                        }
                        .into(),
                    ),
                )
                .into(),
            )
            .into(),
            Box::new(
                CompoundSelector {
                    name: CompoundSelectorName::Benchmark,
                    matcher_name: MatcherName::EndsWith,
                    matcher_parameter: "jo".into(),
                }
                .into(),
            ),
        ),
    );
}
