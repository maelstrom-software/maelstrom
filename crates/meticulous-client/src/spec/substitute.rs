//! Provide [`substitute`] function to do variable substitutions in strings.
use std::{
    borrow::{Borrow, Cow},
    error, fmt, result,
};

/// Error type returned by [`substitute`].
#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    /// The input string had an unterminated curly brace.
    UnterminatedBrace,

    /// The input string had a `$` that wasn't followed by `env{` or `prev{`.
    InvalidVariable,

    /// The input string had a `$env{` or `$prev{` substitution for `var` where the lookup closure
    /// couldn't find `var`.
    UnknownVariable { var: String },

    /// The input string had a `$env{` substitution for `var` where the lookup closure returned an
    /// error, `err`.
    LookupError { var: String, err: String },
}

/// Return type of [`substitute`].
pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnterminatedBrace => write!(f, "unterminated {{"),
            Error::InvalidVariable => {
                write!(f, r#"$ must be followed by "$", "env{{", or "prev{{""#)
            }
            Error::UnknownVariable { var } => write!(f, "unknown variable {var:?}"),
            Error::LookupError { var, err } => write!(f, "error lookup up variable {var:?}: {err}"),
        }
    }
}

impl error::Error for Error {}

fn handle_variable<'a, 'b, LookupT, VarT>(
    result: &'a mut String,
    input: &'b str,
    lookup: LookupT,
) -> Result<()>
where
    LookupT: Fn(&'b str) -> anyhow::Result<Option<VarT>>,
    VarT: Borrow<str>,
{
    let (var, rest) = input.split_once('}').ok_or(Error::UnterminatedBrace)?;

    let (var, default) = if let Some((head, tail)) = var.split_once(":-") {
        (head, Some(tail))
    } else {
        (var, None)
    };

    let val = lookup(var).map_err(|err| Error::LookupError {
        var: var.to_string(),
        err: format!("{err}"),
    })?;
    match (val, default) {
        (None, Some(default)) => {
            result.push_str(default);
        }
        (Some(val), Some(default)) if val.borrow().is_empty() => {
            result.push_str(default);
        }
        (None, _) => {
            return Err(Error::UnknownVariable {
                var: var.to_string(),
            });
        }
        (Some(val), _) => {
            result.push_str(val.borrow());
        }
    }
    result.push_str(rest);
    Ok(())
}

/// Substitute `$env{var}` and `$prev{var}` uses in a string.
///
/// `$env{var}` is used to evaluate environment variables. The `env_lookup` closure provides the
/// means for looking up environment variables. It is allowed to return an error to indicate an
/// actual error, like the variable not being valid UTF-8. In this case, the error will be wrapped
/// in [`Error::LookupError`]. It is also allowed to return `None` to indicate there is no such
/// variable.
///
/// `$prev{var}` is used to for previous values. This could be useful adding something on to a
/// environment variable like `PATH`, where the new value could be `$prev{PATH}:/some/new/path`.
/// The `prev_lookup` closure provides the means for looking up previous variables. It is not
/// allowed to return an error, but it can return `None` to indicate no such variable.
///
/// If either variable lookup closure returns `None`, this is treated as an error. An error of type
/// [`Error::UnknownVariable`] will be returned. If the caller wants to treat missing variables as
/// empty strings, they should do so in the loopup closures.
pub fn substitute<'a, EnvLookupT, EnvVarT, PrevLookupT, PrevVarT>(
    input: &'a str,
    env_lookup: EnvLookupT,
    prev_lookup: PrevLookupT,
) -> Result<Cow<'a, str>>
where
    EnvLookupT: Fn(&'a str) -> anyhow::Result<Option<EnvVarT>>,
    EnvVarT: Borrow<str>,
    PrevLookupT: Fn(&'a str) -> Option<PrevVarT>,
    PrevVarT: Borrow<str>,
{
    let mut iter = input.split('$').peekable();
    let first = iter.next().unwrap();

    if iter.peek().is_none() {
        // There was no dollar sign. Just return the original string.
        return Ok(input.into());
    }

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
            handle_variable(&mut result, rest, |k| Ok(prev_lookup(k)))?;
        } else {
            return Err(Error::InvalidVariable);
        }
    }
    Ok(result.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use meticulous_test::string;

    fn env_lookup(key: &str) -> anyhow::Result<Option<String>> {
        match key {
            "FOO" => Ok(Some(string!("foo"))),
            "BAR" => Ok(Some(string!("bar"))),
            "EMPTY" => Ok(Some(string!(""))),
            "ERROR" => Err(anyhow!("an error occurred")),
            _ => Ok(None),
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
            Error::UnknownVariable {
                var: string!("UNKNOWN")
            },
        );
    }

    #[test]
    fn lookup_error() {
        assert_eq!(
            substitute("$env{ERROR}", env_lookup, prev_lookup).unwrap_err(),
            Error::LookupError {
                var: string!("ERROR"),
                err: string!("an error occurred")
            },
        );
    }
}
