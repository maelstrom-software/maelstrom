use crate::WindowSize;
use std::mem;

pub fn encode_input(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    bytes
        .split(|b| *b == 0xff)
        .enumerate()
        .flat_map(|(n, bytes)| {
            let mut vec = Vec::<&[u8]>::new();
            if n > 0 {
                vec.push(b"\xff\xff");
            }
            if !bytes.is_empty() {
                vec.push(bytes);
            }
            vec
        })
}

pub fn encode_window_size_change(window_size: WindowSize) -> [u8; 6] {
    let rows = window_size.rows.to_be_bytes();
    let columns = window_size.columns.to_be_bytes();
    [0xff, 0, rows[0], rows[1], columns[0], columns[1]]
}

pub trait DecodeInputAcceptor<E> {
    fn input(&mut self, input: &[u8]) -> Result<(), E>;
    fn window_size_change(&mut self, window_size: WindowSize) -> Result<(), E>;
}

#[derive(Debug, Default, PartialEq)]
pub struct DecodeInputRemainder {
    data: [u8; 5],
    length: u8,
}

impl DecodeInputRemainder {
    pub fn new(remainder: &[u8]) -> Self {
        let mut res: Self = Default::default();
        let length = remainder.len();
        assert!(length <= mem::size_of_val(&res.data));
        res.data[..length].copy_from_slice(remainder);
        res.length = length as u8;
        res
    }

    pub fn move_to_slice(&mut self, dest: &mut [u8]) -> usize {
        let length = self.len();
        dest[..length].copy_from_slice(&self.data[..length]);
        *self = Self::default();
        length
    }

    pub fn len(&self) -> usize {
        self.length.into()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, PartialEq)]
pub enum DecodeInputChunk<'a> {
    Input(&'a [u8]),
    WindowSizeChange(WindowSize),
    Remainder(DecodeInputRemainder),
}

pub struct DecodeInputIterator<'a>(&'a [u8]);

impl<'a> Iterator for DecodeInputIterator<'a> {
    type Item = DecodeInputChunk<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.0 {
            [] => None,
            [0xff, 0xff, rest @ ..] => {
                let res = Some(DecodeInputChunk::Input(&self.0[1..2]));
                self.0 = rest;
                res
            }
            [0xff, 0, rh, rl, ch, cl, rest @ ..] => {
                let res = Some(DecodeInputChunk::WindowSizeChange(WindowSize::new(
                    u16::from_be_bytes([*rh, *rl]),
                    u16::from_be_bytes([*ch, *cl]),
                )));
                self.0 = rest;
                res
            }
            [0xff, 0, ..] | [0xff] => {
                let res = Some(DecodeInputChunk::Remainder(DecodeInputRemainder::new(
                    self.0,
                )));
                self.0 = b"";
                res
            }
            [0xff, _, rest @ ..] => {
                let res = Some(DecodeInputChunk::Input(&self.0[..2]));
                self.0 = rest;
                res
            }
            _ => {
                let next_0xff = self
                    .0
                    .iter()
                    .position(|b| *b == 0xff)
                    .unwrap_or(self.0.len());
                let res = Some(DecodeInputChunk::Input(&self.0[..next_0xff]));
                self.0 = &self.0[next_0xff..];
                res
            }
        }
    }
}

pub fn decode_input(input: &[u8]) -> DecodeInputIterator {
    DecodeInputIterator(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use DecodeInputChunk::*;

    fn assert_encode_input<const N: usize>(input: &[u8], expected: [&[u8]; N]) {
        assert_eq!(
            Vec::from_iter(encode_input(input)),
            Vec::from_iter(expected)
        );
    }

    #[test]
    fn encode_input_empty() {
        assert_encode_input(b"", []);
    }

    #[test]
    fn encode_input_basic() {
        assert_encode_input(b"abc", [b"abc"]);
    }

    #[test]
    fn encode_input_leading_escape() {
        assert_encode_input(b"\xffabcdef", [b"\xff\xff", b"abcdef"]);
    }

    #[test]
    fn encode_input_leading_escapes() {
        assert_encode_input(
            b"\xff\xff\xffabcdef",
            [b"\xff\xff", b"\xff\xff", b"\xff\xff", b"abcdef"],
        );
    }

    #[test]
    fn encode_input_only_escape() {
        assert_encode_input(b"\xff", [b"\xff\xff"]);
    }

    #[test]
    fn encode_input_only_escapes() {
        assert_encode_input(b"\xff\xff\xff", [b"\xff\xff", b"\xff\xff", b"\xff\xff"]);
    }

    #[test]
    fn encode_input_trailing_escape() {
        assert_encode_input(b"abcdef\xff", [b"abcdef", b"\xff\xff"]);
    }

    #[test]
    fn encode_input_trailing_escapes() {
        assert_encode_input(
            b"abcdef\xff\xff\xff",
            [b"abcdef", b"\xff\xff", b"\xff\xff", b"\xff\xff"],
        );
    }

    #[test]
    fn encode_input_middle_escape() {
        assert_encode_input(b"abc\xffdef", [b"abc", b"\xff\xff", b"def"]);
    }

    #[test]
    fn encode_input_middle_escapes() {
        assert_encode_input(
            b"abc\xff\xff\xffdef",
            [b"abc", b"\xff\xff", b"\xff\xff", b"\xff\xff", b"def"],
        );
    }

    #[test]
    fn encode_input_escapes() {
        assert_encode_input(
            b"\xffabc\xff\xff\xffdef\xff\xff",
            [
                b"\xff\xff",
                b"abc",
                b"\xff\xff",
                b"\xff\xff",
                b"\xff\xff",
                b"def",
                b"\xff\xff",
                b"\xff\xff",
            ],
        );
    }

    #[test]
    fn encode_window_size_change_basic() {
        assert_eq!(
            encode_window_size_change(WindowSize::new(0x89ab, 0xcdef)),
            *b"\xff\x00\x89\xab\xcd\xef"
        );
    }

    fn assert_decode_input<const N: usize>(input: &[u8], expected: [DecodeInputChunk; N]) {
        assert_eq!(
            Vec::from_iter(decode_input(input)),
            Vec::from_iter(expected),
        );
    }

    #[test]
    fn decode_input_empty() {
        assert_decode_input(b"", []);
    }

    #[test]
    fn decode_input_no_escapes() {
        assert_decode_input(b"abcdef", [Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_short_1_last() {
        assert_decode_input(
            b"abcdef\xff",
            [
                Input(b"abcdef"),
                Remainder(DecodeInputRemainder::new(b"\xff")),
            ],
        );
    }

    #[test]
    fn decode_input_escape_window_short_4_last() {
        assert_decode_input(
            b"abcdef\xff\x00",
            [
                Input(b"abcdef"),
                Remainder(DecodeInputRemainder::new(b"\xff\x00")),
            ],
        );
    }

    #[test]
    fn decode_input_escape_window_short_3_last() {
        assert_decode_input(
            b"abcdef\xff\x00\x89",
            [
                Input(b"abcdef"),
                Remainder(DecodeInputRemainder::new(b"\xff\x00\x89")),
            ],
        );
    }

    #[test]
    fn decode_input_escape_window_short_2_last() {
        assert_decode_input(
            b"abcdef\xff\x00\x89\xab",
            [
                Input(b"abcdef"),
                Remainder(DecodeInputRemainder::new(b"\xff\x00\x89\xab")),
            ],
        );
    }

    #[test]
    fn decode_input_escape_window_short_1_last() {
        assert_decode_input(
            b"abcdef\xff\x00\x89\xab\xcd",
            [
                Input(b"abcdef"),
                Remainder(DecodeInputRemainder::new(b"\xff\x00\x89\xab\xcd")),
            ],
        );
    }

    #[test]
    fn decode_input_escape_window_last() {
        assert_decode_input(
            b"abcdef\xff\x00\x89\xab\xcd\xef",
            [
                Input(b"abcdef"),
                WindowSizeChange(WindowSize::new(0x89ab, 0xcdef)),
            ],
        );
    }

    #[test]
    fn decode_input_escape_escape_last() {
        assert_decode_input(b"abcdef\xff\xff", [Input(b"abcdef"), Input(b"\xff")]);
    }

    #[test]
    fn decode_input_escape_garbage_last() {
        assert_decode_input(b"abcdef\xff\x01", [Input(b"abcdef"), Input(b"\xff\x01")]);
    }

    #[test]
    fn decode_input_kitchen_sink() {
        assert_decode_input(
            b"\xff\xff\x00\xff\x00\x89\xab\xcd\xff\xff\xff\xffabc",
            [
                Input(b"\xff"),
                Input(b"\x00"),
                WindowSizeChange(WindowSize::new(0x89ab, 0xcdff)),
                Input(b"\xff"),
                Input(b"\xffa"),
                Input(b"bc"),
            ],
        );
    }
}
