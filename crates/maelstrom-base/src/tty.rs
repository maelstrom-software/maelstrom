use crate::WindowSize;

pub fn encode_input<E>(
    bytes: &[u8],
    mut writer: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    for (n, bytes) in bytes.split(|b| *b == b'\xff').enumerate() {
        if n > 0 {
            writer(b"\xff\xff")?;
        }
        if !bytes.is_empty() {
            writer(bytes)?;
        }
    }
    Ok(())
}

pub fn encode_window_size_change<E>(
    window_size: WindowSize,
    mut writer: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    let rows = window_size.rows.to_be_bytes();
    let columns = window_size.columns.to_be_bytes();
    let message = [255, 0, rows[0], rows[1], columns[0], columns[1]];
    writer(message.as_slice())
}

pub trait DecodeInputAcceptor<E> {
    fn input(&mut self, input: &[u8]) -> Result<(), E>;
    fn window_size_change(&mut self, window_size: WindowSize) -> Result<(), E>;
}

pub fn decode_input<E>(
    mut input: &[u8],
    mut acceptor: impl DecodeInputAcceptor<E>,
) -> Result<&[u8], E> {
    loop {
        match input.iter().position(|byte| *byte == b'\xff') {
            None => {
                if !input.is_empty() {
                    acceptor.input(input)?;
                }
                return Ok(b"");
            }
            Some(pos) => {
                let first = &input[0..pos];
                if !first.is_empty() {
                    acceptor.input(first)?;
                }
                match &input[pos + 1..] {
                    [0, rh, rl, ch, cl, rest @ ..] => {
                        acceptor.window_size_change(WindowSize::new(
                            u16::from_be_bytes([*rh, *rl]),
                            u16::from_be_bytes([*ch, *cl]),
                        ))?;
                        input = rest;
                    }
                    [0, ..] | [] => {
                        return Ok(&input[pos..]);
                    }
                    [b'\xff', rest @ ..] => {
                        acceptor.input(&input[pos + 1..pos + 2])?;
                        input = rest;
                    }
                    [_, rest @ ..] => {
                        acceptor.input(&input[pos..pos + 2])?;
                        input = rest;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Writer<E = ()> {
        writes: Vec<Vec<u8>>,
        writes_before_error: usize,
        error: E,
    }

    impl<E: Clone> Writer<E> {
        fn new(writes_before_error: usize, error: E) -> Self {
            Self {
                writes: Default::default(),
                writes_before_error,
                error,
            }
        }

        fn writer(&mut self) -> impl FnMut(&[u8]) -> Result<(), E> + '_ {
            |bytes| {
                if self.writes_before_error == 0 {
                    Err(self.error.clone())
                } else {
                    self.writes_before_error -= 1;
                    self.writes.push(Vec::from(bytes));
                    Ok(())
                }
            }
        }

        fn assert_eq<const N: usize>(&self, expected_writes: [&[u8]; N]) {
            let expected = Vec::from_iter(expected_writes.into_iter().map(|slice| slice.to_vec()));
            assert_eq!(self.writes, expected);
        }
    }

    impl Default for Writer<()> {
        fn default() -> Self {
            Self::new(usize::MAX, ())
        }
    }

    #[test]
    fn encode_input_empty() {
        let mut writer = Writer::default();
        encode_input(b"", writer.writer()).unwrap();
        writer.assert_eq([]);
    }

    #[test]
    fn encode_input_basic() {
        let mut writer = Writer::default();
        encode_input(b"abc", writer.writer()).unwrap();
        writer.assert_eq([b"abc"]);
    }

    #[test]
    fn encode_input_leading_escape() {
        let mut writer = Writer::default();
        encode_input(b"\xffabcdef", writer.writer()).unwrap();
        writer.assert_eq([b"\xff\xff", b"abcdef"]);
    }

    #[test]
    fn encode_input_leading_escapes() {
        let mut writer = Writer::default();
        encode_input(b"\xff\xff\xffabcdef", writer.writer()).unwrap();
        writer.assert_eq([b"\xff\xff", b"\xff\xff", b"\xff\xff", b"abcdef"]);
    }

    #[test]
    fn encode_input_only_escape() {
        let mut writer = Writer::default();
        encode_input(b"\xff", writer.writer()).unwrap();
        writer.assert_eq([b"\xff\xff"]);
    }

    #[test]
    fn encode_input_only_escapes() {
        let mut writer = Writer::default();
        encode_input(b"\xff\xff\xff", writer.writer()).unwrap();
        writer.assert_eq([b"\xff\xff", b"\xff\xff", b"\xff\xff"]);
    }

    #[test]
    fn encode_input_trailing_escape() {
        let mut writer = Writer::default();
        encode_input(b"abcdef\xff", writer.writer()).unwrap();
        writer.assert_eq([b"abcdef", b"\xff\xff"]);
    }

    #[test]
    fn encode_input_trailing_escapes() {
        let mut writer = Writer::default();
        encode_input(b"abcdef\xff\xff\xff", writer.writer()).unwrap();
        writer.assert_eq([b"abcdef", b"\xff\xff", b"\xff\xff", b"\xff\xff"]);
    }

    #[test]
    fn encode_input_middle_escape() {
        let mut writer = Writer::default();
        encode_input(b"abc\xffdef", writer.writer()).unwrap();
        writer.assert_eq([b"abc", b"\xff\xff", b"def"]);
    }

    #[test]
    fn encode_input_middle_escapes() {
        let mut writer = Writer::default();
        encode_input(b"abc\xff\xff\xffdef", writer.writer()).unwrap();
        writer.assert_eq([b"abc", b"\xff\xff", b"\xff\xff", b"\xff\xff", b"def"]);
    }

    #[test]
    fn encode_input_escapes() {
        let mut writer = Writer::default();
        encode_input(b"\xffabc\xff\xff\xffdef\xff\xff", writer.writer()).unwrap();
        writer.assert_eq([
            b"\xff\xff",
            b"abc",
            b"\xff\xff",
            b"\xff\xff",
            b"\xff\xff",
            b"def",
            b"\xff\xff",
            b"\xff\xff",
        ]);
    }

    #[test]
    fn encode_input_error() {
        let mut writer = Writer::new(2, "error");
        let result = encode_input(b"abc\xffdef", writer.writer());
        assert_eq!(result, Err("error"));
        writer.assert_eq([b"abc", b"\xff\xff"]);
    }

    #[test]
    fn encode_window_size_change_basic() {
        let mut writer = Writer::default();
        encode_window_size_change(WindowSize::new(0x89ab, 0xcdef), writer.writer()).unwrap();
        writer.assert_eq([b"\xff\x00\x89\xab\xcd\xef"]);
    }

    #[test]
    fn encode_window_size_change_error() {
        let mut writer = Writer::new(0, "error");
        let result = encode_window_size_change(WindowSize::new(0x89ab, 0xcdef), writer.writer());
        assert_eq!(result, Err("error"));
        writer.assert_eq([]);
    }

    #[derive(Debug, PartialEq)]
    enum Chunk<T> {
        Input(T),
        WindowSizeChange(WindowSize),
    }

    struct Acceptor<E = ()> {
        chunks: Vec<Chunk<Box<[u8]>>>,
        chunks_before_error: usize,
        error: E,
    }

    impl<E: Clone> Acceptor<E> {
        fn new(chunks_before_error: usize, error: E) -> Self {
            Self {
                chunks: Default::default(),
                chunks_before_error,
                error,
            }
        }

        fn assert_eq<const N: usize>(&self, expected_chunks: [Chunk<&[u8]>; N]) {
            let expected = Vec::from_iter(expected_chunks.into_iter().map(|chunk| match chunk {
                Chunk::Input(input) => Chunk::Input(Box::from(input)),
                Chunk::WindowSizeChange(window_size) => Chunk::WindowSizeChange(window_size),
            }));
            assert_eq!(self.chunks, expected);
        }

        fn check_error(&mut self) -> Result<(), E> {
            if self.chunks_before_error == 0 {
                Err(self.error.clone())
            } else {
                Ok(())
            }
        }
    }

    impl Default for Acceptor<()> {
        fn default() -> Self {
            Self::new(usize::MAX, ())
        }
    }

    impl<E: Clone> DecodeInputAcceptor<E> for &mut Acceptor<E> {
        fn input(&mut self, input: &[u8]) -> Result<(), E> {
            self.check_error()?;
            self.chunks.push(Chunk::Input(Box::from(input)));
            Ok(())
        }

        fn window_size_change(&mut self, window_size: WindowSize) -> Result<(), E> {
            self.check_error()?;
            self.chunks.push(Chunk::WindowSizeChange(window_size));
            Ok(())
        }
    }

    #[test]
    fn decode_input_empty() {
        let mut acceptor = Acceptor::default();
        assert_eq!(decode_input(b"", &mut acceptor), Ok(&b""[..]));
        acceptor.assert_eq([]);
    }

    #[test]
    fn decode_input_no_escapes() {
        let mut acceptor = Acceptor::default();
        assert_eq!(decode_input(b"abcdef", &mut acceptor), Ok(&b""[..]));
        acceptor.assert_eq([Chunk::Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_short_1_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(decode_input(b"abcdef\xff", &mut acceptor), Ok(&b"\xff"[..]));
        acceptor.assert_eq([Chunk::Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_window_short_4_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(
            decode_input(b"abcdef\xff\x00", &mut acceptor),
            Ok(&b"\xff\x00"[..])
        );
        acceptor.assert_eq([Chunk::Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_window_short_3_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(
            decode_input(b"abcdef\xff\x00\x89", &mut acceptor),
            Ok(&b"\xff\x00\x89"[..])
        );
        acceptor.assert_eq([Chunk::Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_window_short_2_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(
            decode_input(b"abcdef\xff\x00\x89\xab", &mut acceptor),
            Ok(&b"\xff\x00\x89\xab"[..])
        );
        acceptor.assert_eq([Chunk::Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_window_short_1_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(
            decode_input(b"abcdef\xff\x00\x89\xab\xcd", &mut acceptor),
            Ok(&b"\xff\x00\x89\xab\xcd"[..])
        );
        acceptor.assert_eq([Chunk::Input(b"abcdef")]);
    }

    #[test]
    fn decode_input_escape_window_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(
            decode_input(b"abcdef\xff\x00\x89\xab\xcd\xef", &mut acceptor),
            Ok(&b""[..])
        );
        acceptor.assert_eq([
            Chunk::Input(b"abcdef"),
            Chunk::WindowSizeChange(WindowSize::new(0x89ab, 0xcdef)),
        ]);
    }

    #[test]
    fn decode_input_escape_escape_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(decode_input(b"abcdef\xff\xff", &mut acceptor), Ok(&b""[..]));
        acceptor.assert_eq([Chunk::Input(b"abcdef"), Chunk::Input(b"\xff")]);
    }

    #[test]
    fn decode_input_escape_garbage_last() {
        let mut acceptor = Acceptor::default();
        assert_eq!(decode_input(b"abcdef\xff\x01", &mut acceptor), Ok(&b""[..]));
        acceptor.assert_eq([Chunk::Input(b"abcdef"), Chunk::Input(b"\xff\x01")]);
    }

    #[test]
    fn decode_input_kitchen_sink() {
        let mut acceptor = Acceptor::default();
        assert_eq!(
            decode_input(
                b"\xff\xff\x00\xff\x00\x89\xab\xcd\xff\xff\xff\xffabc",
                &mut acceptor
            ),
            Ok(&b""[..])
        );
        acceptor.assert_eq([
            Chunk::Input(b"\xff"),
            Chunk::Input(b"\x00"),
            Chunk::WindowSizeChange(WindowSize::new(0x89ab, 0xcdff)),
            Chunk::Input(b"\xff"),
            Chunk::Input(b"\xffa"),
            Chunk::Input(b"bc"),
        ]);
    }
}
