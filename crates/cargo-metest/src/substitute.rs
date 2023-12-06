use std::{borrow::Cow, error, fmt, result};

#[derive(Debug, Eq, PartialEq)]
pub enum Error<'a> {
    UnterminatedBrace,
    InvalidVariable,
    UnknownVariable(&'a str),
}

pub type Result<'a, T> = result::Result<T, Error<'a>>;

impl<'a> fmt::Display for Error<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnterminatedBrace => write!(f, "unterminated {{"),
            Error::InvalidVariable => {
                write!(f, r#"$ must be followed by "$", "env{{", or "prev{{""#)
            }
            Error::UnknownVariable(var) => write!(f, "unknown variable {var}"),
        }
    }
}

impl<'a> error::Error for Error<'a> {}

fn handle_variable<'a, 'b, 'c>(
    result: &'a mut String,
    input: &'b str,
    lookup: impl Fn(&'b str) -> Option<&'c str>,
) -> Result<'b, ()> {
    let (var, rest) = input
        .split_once('}')
        .map_or_else(|| Err(Error::UnterminatedBrace), Ok)?;
    let expansion = match var.split_once(":-") {
        Some((var, default)) => match lookup(var) {
            Some("") | None => default,
            Some(val) => val,
        },
        None => lookup(var).map_or_else(|| Err(Error::UnknownVariable(var)), Ok)?,
    };
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
                        return Err(Error::InvalidVariable);
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
                return Err(Error::InvalidVariable);
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
        assert_eq!(
            substitute("$", env_lookup, prev_lookup).unwrap_err(),
            Error::InvalidVariable,
        );
    }

    #[test]
    fn trailing_dollar_sign() {
        assert_eq!(
            substitute("foo$", env_lookup, prev_lookup).unwrap_err(),
            Error::InvalidVariable,
        );
    }

    #[test]
    fn trailing_odd_number_of_dollar_signs() {
        assert_eq!(
            substitute("foo$$$$$", env_lookup, prev_lookup).unwrap_err(),
            Error::InvalidVariable,
        );
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

    #[test]
    fn env_prev_defaults() {
        assert_eq!(
            substitute(
                "$env{EMPTY:-Hello} $prev{X:-World}: $env{FOO:-bar}",
                env_lookup,
                prev_lookup
            )
            .unwrap(),
            "Hello World: foo"
        );
    }

    #[test]
    fn unterminated_brace() {
        assert_eq!(
            substitute("$env{FOO $env{BAR}", env_lookup, prev_lookup).unwrap_err(),
            Error::UnterminatedBrace,
        );
    }

    #[test]
    fn unterminated_brace_at_end() {
        assert_eq!(
            substitute("$env{FOO} $env{BAR", env_lookup, prev_lookup).unwrap_err(),
            Error::UnterminatedBrace,
        );
    }

    #[test]
    fn not_env_or_prev() {
        assert_eq!(
            substitute("$FOO", env_lookup, prev_lookup).unwrap_err(),
            Error::InvalidVariable,
        );
    }

    #[test]
    fn not_env_or_prev_2() {
        assert_eq!(
            substitute("$blah{FOO}", env_lookup, prev_lookup).unwrap_err(),
            Error::InvalidVariable,
        );
    }

    #[test]
    fn unknown_variable() {
        assert_eq!(
            substitute("$env{UNKNOWN}", env_lookup, prev_lookup).unwrap_err(),
            Error::UnknownVariable("UNKNOWN"),
        );
    }
}
