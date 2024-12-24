use anyhow::{anyhow, Result};
use regex::Regex;
use std::{borrow::Borrow, collections::HashMap, sync::OnceLock};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ident(String);

impl Ident {
    pub fn new(value: &str) -> Self {
        static IDENT_RE: OnceLock<Regex> = OnceLock::new();
        let ident_re = IDENT_RE.get_or_init(|| Regex::new("^[a-zA-Z-][a-zA-Z0-9-]*$").unwrap());
        if !ident_re.is_match(value) {
            panic!("invalid identifier {value:?}");
        }
        Self(value.to_owned())
    }
}

impl<'a> From<&'a str> for Ident {
    fn from(v: &'a str) -> Self {
        Self::new(v)
    }
}

impl Borrow<str> for Ident {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Default)]
pub struct TemplateVars(HashMap<Ident, String>);

impl TemplateVars {
    pub fn new<I, K, V>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Ident>,
        V: Into<String>,
    {
        Ok(TemplateVars(HashMap::from_iter(
            iter.into_iter().map(|(k, v)| (k.into(), v.into())),
        )))
    }
}

pub fn replace_template_vars(input: &str, vars: &TemplateVars) -> Result<String> {
    static TEMPLATE_RE: OnceLock<Regex> = OnceLock::new();
    let template_re = TEMPLATE_RE.get_or_init(|| {
        // Opening angle brackets followed by identifier ending with a single closing angle
        // bracket. We capture all of the leading brackets, since we support escaping brackets by
        // doubling them.
        Regex::new("(?<var><+[a-zA-Z-][a-zA-Z0-9-]*>)").unwrap()
    });
    let mut last = 0;
    let mut output = String::new();
    for cap in template_re.captures_iter(input) {
        let (range, value) = if let Some(m) = cap.name("var") {
            let m_str = m.as_str();
            let starting_angle = m_str.chars().take_while(|c| *c == '<').count();
            if starting_angle % 2 == 1 {
                let ident = &m_str[starting_angle..(m_str.len() - 1)];
                let new_angle = "<".repeat((starting_angle - 1) / 2);
                let value = vars
                    .0
                    .get(ident)
                    .ok_or_else(|| anyhow!("unknown template variable {ident:?}"))?
                    .as_str();
                (m.range(), format!("{new_angle}{value}"))
            } else {
                let start = m.range().start;
                (
                    start..(start + starting_angle),
                    "<".repeat(starting_angle / 2).to_string(),
                )
            }
        } else {
            unreachable!()
        };

        output += &input[last..range.start];
        output += &value;
        last = range.end;
    }
    output += &input[last..];

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_valid() {
        // Dash okay.
        Ident::new("foo-bar");
        // Mixed case okay. Numbers okay if not at beginning.
        Ident::new("foo-Bar12");
    }

    #[test]
    #[should_panic(expected = r#"invalid identifier "foo_bar""#)]
    fn ident_invalid_has_underscore() {
        // No underscore.
        Ident::new("foo_bar");
    }

    #[test]
    #[should_panic(expected = r#"invalid identifier "1foo-bar""#)]
    fn ident_invalid_starts_with_number() {
        // Can't start with number.
        Ident::new("1foo-bar");
    }

    fn template_success_test(key: &str, value: &str, template: &str, expected: &str) {
        let vars = TemplateVars::new([(key, value)]).unwrap();
        let actual = replace_template_vars(template, &vars).unwrap();
        assert_eq!(expected, &actual);
    }

    fn template_failure_test(key: &str, value: &str, template: &str, expected_error: &str) {
        let vars = TemplateVars::new([(key, value)]).unwrap();
        let err = replace_template_vars(template, &vars).unwrap_err();
        assert_eq!(expected_error, err.to_string());
    }

    #[test]
    fn template_successful_replacement() {
        template_success_test("foo", "bar", "<foo>/baz", "bar/baz");
        template_success_test("foo", "bar", "/hello/<foo>/baz", "/hello/bar/baz");
        template_success_test("foo", "bar", "/hello/<foo>/<foo>", "/hello/bar/bar");
    }

    #[test]
    fn template_replace_many() {
        let vars =
            TemplateVars::new([("food", "apple pie"), ("drink", "coke"), ("name", "bob")]).unwrap();
        let template = "echo '<name> ate <food> while drinking <drink>' > message";
        let actual = replace_template_vars(template, &vars).unwrap();
        assert_eq!(
            "echo 'bob ate apple pie while drinking coke' > message",
            &actual
        );
    }

    #[test]
    fn template_failure() {
        template_failure_test(
            "foo",
            "bar",
            "<poo>/baz",
            "unknown template variable \"poo\"",
        );
    }

    #[test]
    fn template_escaping() {
        template_success_test("foo", "bar", "/he<<llo>/<foo>/<foo>", "/he<llo>/bar/bar");
        template_success_test("foo", "bar", "/he<<llo>/<foo>/<foo>", "/he<llo>/bar/bar");
        template_success_test("foo", "bar", "/he<<<foo>", "/he<bar");
        template_success_test("foo", "bar", "/he<<<foo>>", "/he<bar>");
        template_success_test("foo", "bar", "/he<<<<foo>>", "/he<<foo>>");
        template_success_test("foo", "bar", "/he<<", "/he<<");
        template_success_test("foo", "bar", "/he<some/thing>", "/he<some/thing>");
        template_success_test(
            "foo-dude",
            "bar",
            "/he<<llo>/<foo-dude>/<foo-dude>",
            "/he<llo>/bar/bar",
        );
    }
}
