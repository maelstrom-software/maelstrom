//! When sending keyboard input over a pseudo-terminal, we are presented with a problem: If the
//! user types a control sequence like ^C or ^Z, do we interpret that locally, or send it to the
//! pseudo-terminal for it to interpret. The standard way to deal with this problem is to normally
//! send raw ! sequences over the pseudo-terminal, but to have a special prefix --- an escape
//! sequence --- that indicates that the next character should be interpretted locally instead of
//! being sent. This module provides types and functions to encode and decode such escape
//! sequences.

use ascii::AsAsciiStr as _;
use derive_more::Debug;
use maelstrom_util::config::common::StringError;
use serde::Deserialize;
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

/// The type yielded by [`decode_escapes`].
#[derive(Debug, PartialEq)]
pub enum EscapeChunk<'a> {
    /// A chunk of bytes to be passed to the pseudo-terminal untouched.
    Bytes(&'a [u8]),

    /// A `^C` character. This will be detected when the user types `<escape-char>` folled by `^C`.
    ControlC,

    /// A `^Z` character. This will be detected when the user types `<escape-char>` folled by `^Z`.
    ControlZ,

    /// A single `<escape-char>` at the end of the slice. We don't have enough information yet to
    /// know how to handle it.
    Remainder,
}

/// Iterator type returned by [`decode_escapes`]. It yields [`EscapeChunk`]s.
pub struct EscapeIterator<'a> {
    bytes: &'a [u8],
    escape_char: u8,
}

impl<'a> Iterator for EscapeIterator<'a> {
    type Item = EscapeChunk<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let res;
        if self.bytes.is_empty() {
            res = None;
        } else if self.bytes[0] == self.escape_char {
            if self.bytes.len() == 1 {
                res = Some(EscapeChunk::Remainder);
                self.bytes = b"";
            } else if self.bytes[1] == b'\x03' {
                res = Some(EscapeChunk::ControlC);
                self.bytes = &self.bytes[2..];
            } else if self.bytes[1] == b'\x1a' {
                res = Some(EscapeChunk::ControlZ);
                self.bytes = &self.bytes[2..];
            } else if self.bytes[1] == self.escape_char {
                res = Some(EscapeChunk::Bytes(&self.bytes[1..2]));
                self.bytes = &self.bytes[2..];
            } else {
                let remainder = &self.bytes[2..];
                let next_escape = 2 + remainder
                    .iter()
                    .position(|b| *b == self.escape_char)
                    .unwrap_or(remainder.len());
                res = Some(EscapeChunk::Bytes(&self.bytes[..next_escape]));
                self.bytes = &self.bytes[next_escape..];
            }
        } else {
            let next_escape = self
                .bytes
                .iter()
                .position(|b| *b == self.escape_char)
                .unwrap_or(self.bytes.len());
            res = Some(EscapeChunk::Bytes(&self.bytes[..next_escape]));
            self.bytes = &self.bytes[next_escape..];
        }
        res
    }
}

/// Process a slice of bytes looking for escape sequences. This function always starts in a state
/// where it hasn't just seen an `escape_char`. If the caller wants to start with an `escape_char`,
/// it should make it the first byte in `bytes`.
///
/// This function looks for the special sequences `escape_char`-`^C`, `escape_char`-`^Z`, and
/// `escape_char`-`escape_char`. The first two are dealt with by the callers, while the last gets
/// sent as a single `escape_char`. It lets everthing else pass untouched, including `escape_char`
/// followed by any other character.
pub fn decode_escapes(bytes: &[u8], escape_char: u8) -> EscapeIterator {
    EscapeIterator { bytes, escape_char }
}

/// A type to for parsing and printing an escape character configuration value. This type can be
/// parsed using a variety of schemes:
///   - As a raw ASCII character.
///   - Using caret notation (like `^C`).
///   - Using C-style character escapes (like `\n`, `\x1d`).
///
/// It is printed using caret notation.
///
/// The default is `^]`.
#[derive(Clone, Copy, Debug, Deserialize)]
#[debug("{_0:?}")]
pub struct EscapeChar(u8);

impl EscapeChar {
    pub fn as_byte(self) -> u8 {
        self.0
    }

    pub fn from_byte(byte: u8) -> Self {
        Self(byte)
    }
}

impl From<u8> for EscapeChar {
    fn from(byte: u8) -> Self {
        Self(byte)
    }
}

impl Default for EscapeChar {
    fn default() -> Self {
        Self(b'\x1d')
    }
}

impl Display for EscapeChar {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(character) = ascii::caret_encode(self.0) {
            Display::fmt(&format!("^{}", char::from(character.as_byte())), f)
        } else {
            Display::fmt(&char::from(self.0), f)
        }
    }
}

impl FromStr for EscapeChar {
    type Err = StringError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input
            .as_ascii_str()
            .map_err(|err| StringError(format!("invalid character literal: {err}")))?;
        Ok(Self(match input.as_bytes() {
            [] => {
                return Err(StringError::new("invalid character literal: empty string"));
            }
            [c] => *c,
            [b'^', c] => {
                if let Some(character) = ascii::caret_decode(*c) {
                    character.as_byte()
                } else {
                    return Err(StringError::new(
                        "invalid character literal: invalid character after caret",
                    ));
                }
            }
            [b'^', _, ..] => {
                return Err(StringError::new(
                    "invalid character literal: too many characters after caret",
                ));
            }
            [b'\\', b'n'] => b'\n',
            [b'\\', b'r'] => b'\r',
            [b'\\', b't'] => b'\t',
            [b'\\', b'\\'] => b'\\',
            [b'\\', b'0'] => b'\0',
            [b'\\', b'\''] => b'\'',
            [b'\\', b'"'] => b'\"',
            [b'\\', b'x', a @ (b'0'..=b'7'), b @ (b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')] => {
                (char::from(*a).to_digit(16).unwrap() * 16 + char::from(*b).to_digit(16).unwrap())
                    .try_into()
                    .unwrap()
            }
            [b'\\', b'x', b'8'..=b'9' | b'a'..=b'f' | b'A'..=b'F', b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'] =>
            {
                return Err(StringError::new(
                    "invalid character literal: hex escape yields value too large for ASCII",
                ));
            }
            [b'\\', ..] => {
                return Err(StringError::new(
                    "invalid character literal: bad byte escape",
                ));
            }
            [..] => {
                return Err(StringError::new("invalid character literal: too long"));
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use EscapeChunk::*;

    fn assert_decode_escapes<const N: usize>(
        input: &[u8],
        escape_char: u8,
        expected: [EscapeChunk; N],
    ) {
        assert_eq!(
            Vec::from_iter(decode_escapes(input, escape_char)),
            Vec::from_iter(expected),
        );
    }

    #[test]
    fn decode_escapes_empty() {
        assert_decode_escapes(b"", b'!', []);
    }

    #[test]
    fn decode_escapes_no_escapes() {
        assert_decode_escapes(b"abcdef", b'!', [Bytes(b"abcdef")]);
    }

    #[test]
    fn decode_escapes_control_c() {
        assert_decode_escapes(
            b"abc!\x03def",
            b'!',
            [Bytes(b"abc"), ControlC, Bytes(b"def")],
        );
    }

    #[test]
    fn decode_escapes_control_z() {
        assert_decode_escapes(
            b"abc!\x1adef",
            b'!',
            [Bytes(b"abc"), ControlZ, Bytes(b"def")],
        );
    }

    #[test]
    fn decode_escapes_double_escape() {
        assert_decode_escapes(
            b"abc!!def",
            b'!',
            [Bytes(b"abc"), Bytes(b"!"), Bytes(b"def")],
        );
    }

    #[test]
    fn decode_escapes_garbage() {
        assert_decode_escapes(b"abc!xdef", b'!', [Bytes(b"abc"), Bytes(b"!xdef")]);
    }

    #[test]
    fn decode_escapes_short_1() {
        assert_decode_escapes(b"abcdef!", b'!', [Bytes(b"abcdef"), Remainder]);
    }

    #[test]
    fn decode_escapes_multiple_escapes() {
        assert_decode_escapes(
            b"abc!!!!!xdef",
            b'!',
            [Bytes(b"abc"), Bytes(b"!"), Bytes(b"!"), Bytes(b"!xdef")],
        );
    }

    #[track_caller]
    fn assert_parse_escape_char(input: &str, expected: u8) {
        assert_eq!(input.parse::<EscapeChar>().unwrap().as_byte(), expected);
    }

    #[track_caller]
    fn assert_parse_escape_char_error(input: &str, expected_message: &str) {
        assert_eq!(
            input.parse::<EscapeChar>().unwrap_err().to_string(),
            expected_message
        );
    }

    #[test]
    fn parse_escape_char_empty() {
        assert_parse_escape_char_error("", "invalid character literal: empty string");
    }

    #[test]
    fn parse_escape_char_single_char() {
        assert_parse_escape_char("a", b'a');
        assert_parse_escape_char("\x01", b'\x01');
        assert_parse_escape_char("^", b'^');
    }

    #[test]
    fn parse_escape_char_non_ascii() {
        assert_parse_escape_char_error(
            "\u{211a}",
            "invalid character literal: the byte at index 0 is not ASCII",
        );
    }

    #[test]
    fn parse_escape_char_caret_notation() {
        assert_parse_escape_char("^A", b'\x01');
        assert_parse_escape_char("^B", b'\x02');
        assert_parse_escape_char("^\\", b'\x1c');
        assert_parse_escape_char("^@", b'\x00');
    }

    #[test]
    fn parse_escape_char_caret_notation_too_long() {
        assert_parse_escape_char_error(
            "^ab",
            "invalid character literal: too many characters after caret",
        );
    }

    #[test]
    fn parse_escape_char_caret_notation_bad_char() {
        assert_parse_escape_char_error(
            "^a",
            "invalid character literal: invalid character after caret",
        );
        assert_parse_escape_char_error(
            "^&",
            "invalid character literal: invalid character after caret",
        );
    }

    #[test]
    fn parse_escape_char_too_long() {
        assert_parse_escape_char_error("ab", "invalid character literal: too long");
    }

    #[test]
    fn parse_escape_char_backslash_plus_one() {
        assert_parse_escape_char(r#"\n"#, b'\n');
        assert_parse_escape_char(r#"\r"#, b'\r');
        assert_parse_escape_char(r#"\t"#, b'\t');
        assert_parse_escape_char(r#"\\"#, b'\\');
        assert_parse_escape_char(r#"\0"#, b'\0');
        assert_parse_escape_char(r#"\'"#, b'\'');
        assert_parse_escape_char(r#"\""#, b'\"');
    }

    #[test]
    fn parse_escape_char_backslash_hex() {
        assert_parse_escape_char(r#"\x00"#, b'\x00');
        assert_parse_escape_char(r#"\x7f"#, b'\x7f');
        assert_parse_escape_char(r#"\x1A"#, b'\x1A');
        assert_parse_escape_char(r#"\x1b"#, b'\x1b');
    }

    #[test]
    fn parse_escape_char_backslash_hex_too_big() {
        assert_parse_escape_char_error(
            r#"\x80"#,
            "invalid character literal: hex escape yields value too large for ASCII",
        );
        assert_parse_escape_char_error(
            r#"\xaa"#,
            "invalid character literal: hex escape yields value too large for ASCII",
        );
        assert_parse_escape_char_error(
            r#"\xBB"#,
            "invalid character literal: hex escape yields value too large for ASCII",
        );
    }

    #[test]
    fn parse_escape_char_backslash_invalid() {
        assert_parse_escape_char_error(r#"\7"#, "invalid character literal: bad byte escape");
        assert_parse_escape_char_error(r#"\x1234"#, "invalid character literal: bad byte escape");
    }
}
