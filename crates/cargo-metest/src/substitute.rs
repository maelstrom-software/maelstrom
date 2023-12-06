use anyhow::{anyhow, Result};
use std::borrow::Cow;

fn handle_variable<'a, 'b, 'c>(
    result: &'a mut String,
    input: &'b str,
    lookup: impl Fn(&'b str) -> Option<&'c str>,
) -> Result<()> {
    let (var, rest) = input
        .split_once('}')
        .map_or_else(|| Err(anyhow!("unterminated {{")), Ok)?;
    let expansion = lookup(var).map_or_else(|| Err(anyhow!("couldn't find variable {var}")), Ok)?;
    result.push_str(expansion);
    result.push_str(rest);
    Ok(())
}

pub fn substitute<'a, 'b, 'c>(
    input: &'a str,
    env_lookup: impl Fn(&'a str) -> Option<&'b str>,
    prev_lookup: impl Fn(&'a str) -> Option<&'c str>,
) -> Result<Cow<'a, str>> {
    let mut iter = input.split('$').peekable();
    let first = iter.next().unwrap();

    if iter.peek().is_some() {
        let mut result = first.to_string();

        while let Some(subst_str) = iter.next() {
            if subst_str.is_empty() {
                match iter.next() {
                    None => {
                        return Err(anyhow!("string ended with $"));
                    }
                    Some(next) => {
                        result.push('$');
                        result.push_str(next);
                    }
                }
            } else if let Some(rest) = subst_str.strip_prefix("env{") {
                handle_variable(&mut result, rest, &env_lookup)?;
            } else if let Some(rest) = subst_str.strip_prefix("prev{") {
                handle_variable(&mut result, rest, &prev_lookup)?;
            } else {
                return Err(anyhow!("$ must be followed by \"env{{\" or \"prev{{\""));
            }
        }

        Ok(result.into())
    } else {
        // There was no dollar sign. Just return the original string.
        Ok(input.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_lookup(key: &str) -> Option<&str> {
        match key {
            "FOO" => Some("foo"),
            "BAR" => Some("bar"),
            "EMPTY" => Some(""),
            _ => None,
        }
    }

    fn prev_lookup(key: &str) -> Option<&str> {
        match key {
            "FOO" => Some("prev-foo"),
            "BAR" => Some("prev-bar"),
            "EMPTY" => Some(""),
            _ => None,
        }
    }

    #[test]
    fn empty() {
        assert_eq!(substitute("", env_lookup, prev_lookup).unwrap(), "");
    }

    #[test]
    fn no_substitutions() {
        assert_eq!(
            substitute("foo bar blah", env_lookup, prev_lookup).unwrap(),
            "foo bar blah"
        );
    }

    #[test]
    fn double_dollar_signs() {
        assert_eq!(
            substitute("foo$$bar$$$$baz$$$$$$", env_lookup, prev_lookup).unwrap(),
            "foo$bar$$baz$$$"
        );
    }

    #[test]
    fn leading_double_dollar_signs() {
        assert_eq!(
            substitute("$$$$foo$$bar$$$$baz", env_lookup, prev_lookup).unwrap(),
            "$$foo$bar$$baz"
        );
    }

    #[test]
    fn one_dollar_sign() {
        substitute("$", env_lookup, prev_lookup).unwrap_err();
    }

    #[test]
    fn trailing_dollar_sign() {
        substitute("foo$", env_lookup, prev_lookup).unwrap_err();
    }

    #[test]
    fn trailing_odd_number_of_dollar_signs() {
        substitute("foo$$$$$", env_lookup, prev_lookup).unwrap_err();
    }

    #[test]
    fn env_and_prev() {
        assert_eq!(
            substitute(
                "xx$env{FOO}yy$prev{FOO}$env{BAR}$prev{BAR}zz",
                env_lookup,
                prev_lookup
            )
            .unwrap(),
            "xxfooyyprev-foobarprev-barzz"
        );
    }

    #[test]
    fn env_only() {
        assert_eq!(
            substitute("$env{FOO}", env_lookup, prev_lookup).unwrap(),
            "foo"
        );
    }

    #[test]
    fn prev_beginning() {
        assert_eq!(
            substitute("$prev{FOO}more", env_lookup, prev_lookup).unwrap(),
            "prev-foomore"
        );
    }

    #[test]
    fn env_end() {
        assert_eq!(
            substitute("more$env{FOO}", env_lookup, prev_lookup).unwrap(),
            "morefoo"
        );
    }

    #[test]
    fn env_prev_beginning_and_end() {
        assert_eq!(
            substitute("$env{FOO}$prev{BAR}", env_lookup, prev_lookup).unwrap(),
            "fooprev-bar"
        );
    }
}
