use anyhow::{anyhow, bail, Result};
use regex::Regex;
use std::borrow::Borrow;
use std::collections::HashMap;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ident(String);

impl Ident {
    pub fn new(value: &str) -> Result<Self> {
        let ident_re = Regex::new("^[a-zA-Z-][a-zA-Z0-9-]*$").unwrap();
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
    // Dash okay.
    Ident::new("foo-bar").unwrap();
    // Mixed case okay. Numbers okay if not at beginning.
    Ident::new("foo-Bar12").unwrap();
}

#[test]
fn ident_invalid() {
    // No underscore.
    Ident::new("foo_bar").unwrap_err();
    // Can't start with number.
    Ident::new("1foo-bar").unwrap_err();
}

#[derive(Default)]
pub struct TemplateVars(HashMap<Ident, String>);

impl TemplateVars {
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
            # opening angle braces followed by identifier ending with a single closing angle brace
            (?<var><+[a-zA-Z-][a-zA-Z0-9-]*>)
        )
    ",
    )
    .unwrap();
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
fn template_success_test(key: &str, value: &str, template: &str, expected: &str) {
    let vars = TemplateVars::default().with_var(key, value).unwrap();
    let actual = replace_template_vars(template, &vars).unwrap();
    assert_eq!(expected, &actual);
}

#[cfg(test)]
fn template_failure_test(key: &str, value: &str, template: &str, expected_error: &str) {
    let vars = TemplateVars::default().with_var(key, value).unwrap();
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
    let vars = TemplateVars::default()
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
