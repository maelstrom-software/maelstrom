use anyhow::{anyhow, Result};
use regex::Regex;
use std::{collections::HashMap, convert::AsRef, sync::OnceLock};

const IDENT: &str = "[a-zA-Z-][a-zA-Z0-9-]*";

fn validate_ident(ident: &str) {
    static IDENT_RE: OnceLock<Regex> = OnceLock::new();
    let ident_re = IDENT_RE.get_or_init(|| Regex::new(&format!("^{IDENT}$")).unwrap());
    if !ident_re.is_match(ident) {
        panic!("invalid identifier {ident:?}");
    }
}

// Keep this in a function that won't be duplicated for each generic parameter.
fn template_re() -> &'static Regex {
    static TEMPLATE_RE: OnceLock<Regex> = OnceLock::new();
    TEMPLATE_RE.get_or_init(|| {
        // Opening angle brackets followed by identifier ending with a single closing angle
        // bracket. We capture all of the leading brackets, since we support escaping brackets by
        // doubling them.
        Regex::new(&format!("(?<var><+{IDENT}>)")).unwrap()
    })
}

#[derive(Default)]
pub struct TemplateVariables(HashMap<String, String>);

impl TemplateVariables {
    pub fn new<I, K, V>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        TemplateVariables(HashMap::from_iter(iter.into_iter().map(|(k, v)| {
            let k = k.into();
            validate_ident(&k);
            (k, v.into())
        })))
    }

    pub fn replace(&self, input: &(impl AsRef<str> + ?Sized)) -> Result<String> {
        let input = input.as_ref();
        let mut last = 0;
        let mut output = String::new();
        for cap in template_re().captures_iter(input) {
            let (range, value) = if let Some(m) = cap.name("var") {
                let m_str = m.as_str();
                let starting_angle = m_str.chars().take_while(|c| *c == '<').count();
                if starting_angle % 2 == 1 {
                    let ident = &m_str[starting_angle..(m_str.len() - 1)];
                    let new_angle = "<".repeat((starting_angle - 1) / 2);
                    let value = self
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_valid() {
        TemplateVariables::new([
            // Dash okay.
            ("foo-bar", ""),
            // Mixed case okay. Numbers okay if not at beginning.
            ("foo-Bar12", ""),
        ]);
    }

    #[test]
    #[should_panic(expected = r#"invalid identifier "foo_bar""#)]
    fn ident_invalid_has_underscore() {
        TemplateVariables::new([
            // No underscore.
            ("foo_bar", ""),
        ]);
    }

    #[test]
    #[should_panic(expected = r#"invalid identifier "1foo-bar""#)]
    fn ident_invalid_starts_with_number() {
        TemplateVariables::new([
            // Can't start with number.
            ("1foo-bar", ""),
        ]);
    }

    fn template_success_test(key: &str, value: &str, template: &str, expected: &str) {
        let vars = TemplateVariables::new([(key, value)]);
        let actual = vars.replace(template).unwrap();
        assert_eq!(expected, &actual);
    }

    fn template_failure_test(key: &str, value: &str, template: &str, expected_error: &str) {
        let vars = TemplateVariables::new([(key, value)]);
        let err = vars.replace(template).unwrap_err();
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
            TemplateVariables::new([("food", "apple pie"), ("drink", "coke"), ("name", "bob")]);
        let template = "echo '<name> ate <food> while drinking <drink>' > message";
        let actual = vars.replace(template).unwrap();
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
