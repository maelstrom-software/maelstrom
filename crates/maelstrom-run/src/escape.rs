#![allow(bindings_with_variant_name)]

use ascii::AsAsciiStr as _;
use std::{str::FromStr, fmt::{self, Formatter, Debug, Display}};
use maelstrom_util::config::common::StringError;
use serde::Deserialize;

#[derive(Debug, PartialEq)]
pub enum EscapeChunk<'a> {
    Bytes(&'a [u8]),
    ControlC,
    ControlZ,
    Remainder,
}

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

pub fn decode_escapes(bytes: &[u8], escape_char: u8) -> EscapeIterator {
    EscapeIterator { bytes, escape_char }
}

pub fn parse_escape_char(input: &str) -> Result<u8, StringError> {
    let input = input
        .as_ascii_str()
        .map_err(|err| StringError(format!("invalid character literal: {err}")))?;
    match input.as_bytes() {
        [] => Err(StringError::new("invalid character literal: empty string")),
        [c] => Ok(*c),
        [b'^', c] => {
            if let Some(c) = ascii::caret_decode(*c) {
                Ok(c.as_byte())
            } else {
                Err(StringError::new(
                    "invalid character literal: invalid character after caret"
                ))
            }
        }
        [b'^', _, ..] => Err(StringError::new(
            "invalid character literal: too many characters after caret"
        )),
        [b'\\', b'n'] => Ok(b'\n'),
        [b'\\', b'r'] => Ok(b'\r'),
        [b'\\', b't'] => Ok(b'\t'),
        [b'\\', b'\\'] => Ok(b'\\'),
        [b'\\', b'0'] => Ok(b'\0'),
        [b'\\', b'\''] => Ok(b'\''),
        [b'\\', b'"'] => Ok(b'\"'),
        [b'\\', b'x', a @ (b'0'..=b'7'), b @ (b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')] => Ok(
            (char::from(*a).to_digit(16).unwrap() * 16 + char::from(*b).to_digit(16).unwrap())
                .try_into()
                .unwrap(),
        ),
        [b'\\', b'x', b'8'..=b'9' | b'a'..=b'f' | b'A'..=b'F', b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'] => {
            Err(StringError::new("invalid character literal: hex escape yields value too large for ASCII"))
        }
        [b'\\', ..] => {
            Err(StringError::new("invalid character literal: bad byte escape"))
        }
        [..] => Err(StringError::new("invalid character literal: too long")),
    }
}

#[derive(Clone, Copy, Deserialize)]
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

impl Debug for EscapeChar {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Default for EscapeChar {
    fn default() -> Self {
        Self(b'\x1d')
    }
}

impl Display for EscapeChar {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(c) = ascii::caret_encode(self.0) {
            Display::fmt(&format!("^{}", char::from(c.as_byte())), f)
        } else {
            Display::fmt(&char::from(self.0), f)
        }
    }
}

impl FromStr for EscapeChar {
    type Err = StringError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
                parse_escape_char(s)?
        ))
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

    fn assert_parse_escape_char(input: &str, expected: u8) {
        assert_eq!(parse_escape_char(input).unwrap(), expected);
    }

    fn assert_parse_escape_char_error(input: &str, expected_message: &str) {
        assert_eq!(
            parse_escape_char(input).unwrap_err().to_string(),
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
        assert_parse_escape_char_error(
            r#"\7"#,
            "invalid character literal: bad byte escape",
        );
        assert_parse_escape_char_error(
            r#"\x1234"#,
            "invalid character literal: bad byte escape",
        );
    }
}
