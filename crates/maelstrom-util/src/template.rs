use anyhow::{anyhow, bail, Result};
use regex::Regex;
use std::borrow::Borrow;
use std::collections::HashMap;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ident(String);

impl Ident {
    pub fn new(value: &str) -> Result<Self> {
        let ident_re = Regex::new("^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        if !ident_re.is_match(value) {
            bail!("invalid identifier {value:?}");
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'a> TryFrom<&'a str> for Ident {
    type Error = anyhow::Error;

    fn try_from(v: &'a str) -> Result<Self> {
        Self::new(v)
    }
}

impl Borrow<str> for Ident {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

#[test]
fn ident_valid() {
    // Underscore okay.
    Ident::new("foo_bar").unwrap();
    // Mixed case okay. Numbers okay if not at beginning.
    Ident::new("foo_Bar12").unwrap();
}

#[test]
fn ident_invalid() {
    // No dash.
    Ident::new("foo-bar").unwrap_err();
    // Can't start with number.
    Ident::new("1foo_bar").unwrap_err();
}

#[derive(Default)]
pub struct TemplateVars(HashMap<Ident, String>);

impl TemplateVars {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_var(
        mut self,
        key: impl TryInto<Ident, Error = anyhow::Error>,
        value: impl Into<String>,
    ) -> Result<Self> {
        let key = key.try_into()?;
        if self.0.contains_key(&key) {
            bail!("duplicate key {key:?}")
        }
        self.0.insert(key, value.into());
        Ok(self)
    }
}

pub fn replace_template_vars(input: &str, vars: &TemplateVars) -> Result<String> {
    let template_re = Regex::new(
        "(?x) # verbose mode
        (?:
            # non-backslash or start followed by identifier surrounded by carets
            (?:[^\\\\]|^) (?<var><[a-zA-Z_][a-zA-Z0-9_]*>) |

            # simultaneously look for escaped carets to fix
            (?<escape_open>\\\\<) |
            (?<escape_close>\\\\>)
        )
    ",
    )
    .unwrap();
    let mut last = 0;
    let mut output = String::new();
    for cap in template_re.captures_iter(input) {
        let (m, value) = if let Some(m) = cap.name("var") {
            let m_str = m.as_str();
            let ident = &m_str[1..(m_str.len() - 1)];

            (
                m,
                vars.0
                    .get(ident)
                    .ok_or_else(|| anyhow!("unknown template variable {ident:?}"))?
                    .as_str(),
            )
        } else if let Some(m) = cap.name("escape_open") {
            (m, "<")
        } else if let Some(m) = cap.name("escape_close") {
            (m, ">")
        } else {
            unreachable!()
        };

        let range = m.range();
        output += &input[last..range.start];
        output += value;
        last = range.end;
    }
    output += &input[last..];

    Ok(output)
}

#[cfg(test)]
fn template_success_test(key: &str, value: &str, template: &str, expected: &str) {
    let vars = TemplateVars::new().with_var(key, value).unwrap();
    let actual = replace_template_vars(template, &vars).unwrap();
    assert_eq!(expected, &actual);
}

#[cfg(test)]
fn template_failure_test(key: &str, value: &str, template: &str, expected_error: &str) {
    let vars = TemplateVars::new().with_var(key, value).unwrap();
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
    let vars = TemplateVars::new()
        .with_var("food", "apple pie")
        .unwrap()
        .with_var("drink", "coke")
        .unwrap()
        .with_var("name", "bob")
        .unwrap();
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
    template_success_test("foo", "bar", "/he\\<llo>/<foo>/<foo>", "/he<llo>/bar/bar");
    template_success_test("foo", "bar", "/he\\<llo\\>/<foo>/<foo>", "/he<llo>/bar/bar");
    template_success_test("foo", "bar", "/he<llo\\>/<foo>/<foo>", "/he<llo>/bar/bar");
}
