use meticulous_base::Sha256Digest;
use sha2::{Digest as _, Sha256};
use std::io::{self, Chain, Read, Repeat, Take};

/// A reader wrapper that will always return a specific number of bytes, except on error. If the
/// inner, wrapped, reader returns EOF before the specified number of bytes have been returned,
/// this reader will pad the remaining bytes with zeros. If the inner reader returns more bytes
/// than the specified number, this reader will return EOF early, like [Read::take].
pub struct FixedSizeReader<InnerT>(Take<Chain<InnerT, Repeat>>);

impl<InnerT: Read> FixedSizeReader<InnerT> {
    pub fn new(inner: InnerT, limit: u64) -> Self {
        FixedSizeReader(inner.chain(io::repeat(0)).take(limit))
    }

    pub fn into_inner(self) -> InnerT {
        self.0.into_inner().into_inner().0
    }
}

impl<InnerT: Read> Read for FixedSizeReader<InnerT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

pub struct Sha256Reader<InnerT> {
    inner: InnerT,
    hasher: Sha256,
}

impl<InnerT> Sha256Reader<InnerT> {
    pub fn new(inner: InnerT) -> Self {
        Sha256Reader {
            inner,
            hasher: Sha256::new(),
        }
    }

    pub fn finalize(self) -> (InnerT, Sha256Digest) {
        (self.inner, Sha256Digest::new(self.hasher.finalize().into()))
    }
}

impl<InnerT: Read> Read for Sha256Reader<InnerT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(buf)?;
        self.hasher.update(&buf[..size]);
        Ok(size)
    }
}
