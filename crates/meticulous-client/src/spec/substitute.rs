use std::{
    borrow::{Borrow, Cow},
    error, fmt, result,
};

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    UnterminatedBrace,
    InvalidVariable,
    UnknownVariable(String),
    LookupError(String, String),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnterminatedBrace => write!(f, "unterminated {{"),
            Error::InvalidVariable => {
                write!(f, r#"$ must be followed by "$", "env{{", or "prev{{""#)
            }
            Error::UnknownVariable(var) => write!(f, "unknown variable {var:?}"),
            Error::LookupError(var, err) => write!(f, "error lookup up variable {var:?}: {err}"),
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

    let val = lookup(var).map_err(|err| Error::LookupError(var.to_string(), format!("{err}")))?;
    match (val, default) {
        (None, Some(default)) => {
            result.push_str(default);
        }
        (Some(val), Some(default)) if val.borrow().is_empty() => {
            result.push_str(default);
        }
        (None, _) => {
            return Err(Error::UnknownVariable(var.to_string()));
        }
        (Some(val), _) => {
            result.push_str(val.borrow());
        }
    }
    result.push_str(rest);
    Ok(())
}

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
                handle_variable(&mut result, rest, |k| Ok(prev_lookup(k)))?;
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
    use anyhow::anyhow;

    fn env_lookup(key: &str) -> anyhow::Result<Option<String>> {
        match key {
            "FOO" => Ok(Some("foo".to_string())),
            "BAR" => Ok(Some("bar".to_string())),
            "EMPTY" => Ok(Some("".to_string())),
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
            Error::UnknownVariable("UNKNOWN".to_string()),
        );
    }

    #[test]
    fn lookup_error() {
        assert_eq!(
            substitute("$env{ERROR}", env_lookup, prev_lookup).unwrap_err(),
            Error::LookupError("ERROR".to_string(), "an error occurred".to_string()),
        );
    }
}
