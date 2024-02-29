//! Useful [`Read`]ers.

use byteorder::{BigEndian, ReadBytesExt as _, WriteBytesExt as _};
use maelstrom_base::Sha256Digest;
use sha2::{Digest as _, Sha256};
use std::io::{self, Chain, Read, Repeat, Take, Write};
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A [`Read`]er wrapper that will always reads a specific number of bytes, except on error. If the
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

/// A IO wrapper that computes the SHA-256 digest of the bytes that are read from or written to it.
pub struct Sha256Stream<InnerT> {
    inner: InnerT,
    hasher: Sha256,
}

impl<InnerT> Sha256Stream<InnerT> {
    pub fn new(inner: InnerT) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    /// Deconstruct the reader and return the inner reader and computed digest.
    pub fn finalize(self) -> (InnerT, Sha256Digest) {
        (self.inner, Sha256Digest::new(self.hasher.finalize().into()))
    }
}

impl<InnerT: Read> Read for Sha256Stream<InnerT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(buf)?;
        self.hasher.update(&buf[..size]);
        Ok(size)
    }
}

impl<InnerT: Write> Write for Sha256Stream<InnerT> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.inner.write(buf)?;
        self.hasher.update(&buf[..size]);
        Ok(size)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<InnerT: AsyncRead + Unpin> AsyncRead for Sha256Stream<InnerT> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let start_len = dst.filled().len();
        let me = self.get_mut();
        let result = AsyncRead::poll_read(pin!(&mut me.inner), cx, dst);
        if matches!(result, Poll::Ready(Ok(_))) {
            me.hasher.update(&dst.filled()[start_len..]);
        }
        result
    }
}

impl<InnerT: AsyncWrite + Unpin> AsyncWrite for Sha256Stream<InnerT> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        let result = AsyncWrite::poll_write(pin!(&mut me.inner), cx, src);
        if let Poll::Ready(Ok(size)) = &result {
            me.hasher.update(&src[..*size]);
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let me = self.get_mut();
        AsyncWrite::poll_flush(pin!(&mut me.inner), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let me = self.get_mut();
        AsyncWrite::poll_shutdown(pin!(&mut me.inner), cx)
    }
}

#[cfg(test)]
fn calculate_read_hash(mut input: &[u8]) -> Sha256Digest {
    let mut reader = Sha256Stream::new(&mut input);
    std::io::copy(&mut reader, &mut std::io::sink()).unwrap();
    reader.finalize().1
}

#[cfg(test)]
fn calculate_write_hash(mut input: &[u8]) -> Sha256Digest {
    let mut writer = Sha256Stream::new(std::io::sink());
    std::io::copy(&mut input, &mut writer).unwrap();
    writer.finalize().1
}

#[cfg(test)]
async fn calculate_async_read_hash(mut input: &[u8]) -> Sha256Digest {
    let mut reader = Sha256Stream::new(&mut input);
    tokio::io::copy(&mut reader, &mut tokio::io::sink())
        .await
        .unwrap();
    reader.finalize().1
}

#[cfg(test)]
async fn calculate_async_write_hash(mut input: &[u8]) -> Sha256Digest {
    let mut writer = Sha256Stream::new(tokio::io::sink());
    tokio::io::copy(&mut input, &mut writer).await.unwrap();
    writer.finalize().1
}

#[tokio::test]
async fn sha256_stream_impls_consistent() {
    let test_bytes = Vec::from_iter([1, 2, 3, 4, 5, 6, 7].into_iter().cycle().take(1000));
    let read_hash = calculate_read_hash(&test_bytes);
    let write_hash = calculate_write_hash(&test_bytes);
    let async_read_hash = calculate_async_read_hash(&test_bytes).await;
    let async_write_hash = calculate_async_write_hash(&test_bytes).await;

    assert_eq!(read_hash, write_hash);
    assert_eq!(read_hash, async_read_hash);
    assert_eq!(read_hash, async_write_hash);
}

struct Chunk<ReaderT> {
    reader: io::Take<ReaderT>,
}

impl<ReaderT: io::Read> Chunk<ReaderT> {
    fn new(mut reader: ReaderT) -> io::Result<Option<Self>> {
        let size = reader.read_u32::<BigEndian>()?;
        Ok((size != 0).then(|| Self {
            reader: reader.take(size as u64),
        }))
    }

    fn into_inner(self) -> ReaderT {
        self.reader.into_inner()
    }
}

impl<ReaderT: io::Read> io::Read for Chunk<ReaderT> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buffer)
    }
}

pub struct ChunkedReader<ReaderT> {
    reader: Option<ReaderT>,
    chunk: Option<Chunk<ReaderT>>,
}

impl<ReaderT> ChunkedReader<ReaderT> {
    pub fn new(reader: ReaderT) -> Self {
        Self {
            reader: Some(reader),
            chunk: None,
        }
    }
}

impl<ReaderT: io::Read> io::Read for ChunkedReader<ReaderT> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if let Some(mut chunk) = self.chunk.take() {
            let read = chunk.read(buffer)?;
            return if read == 0 {
                self.reader = Some(chunk.into_inner());
                self.read(buffer)
            } else {
                self.chunk.replace(chunk);
                Ok(read)
            };
        } else if let Some(reader) = self.reader.take() {
            if let Some(chunk) = Chunk::new(reader)? {
                self.chunk = Some(chunk);
                return self.read(buffer);
            }
        }
        Ok(0)
    }
}

#[cfg(test)]
fn test_chunked_reader(input: &[u8], expected: &[&[u8]]) -> io::Result<()> {
    let mut reader = ChunkedReader::new(input);
    for e in expected {
        let mut actual = vec![0; e.len()];
        reader.read_exact(&mut actual[..])?;
        assert_eq!(&actual, e);
    }

    let mut rest = vec![];
    reader.read_to_end(&mut rest)?;
    assert!(rest.is_empty(), "{rest:?}");

    Ok(())
}

#[test]
fn chunked_reader() {
    test_chunked_reader(
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 2, 6, 7, 0, 0, 0, 0],
        &[&[1, 2, 3], &[4, 5, 6], &[7]],
    )
    .unwrap();

    test_chunked_reader(
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 2, 6, 7, 0, 0, 0, 0],
        &[&[1, 2, 3, 4, 5], &[6, 7]],
    )
    .unwrap();

    test_chunked_reader(
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 2, 6, 7, 0, 0, 0, 0],
        &[&[1, 2, 3, 4, 5, 6, 7]],
    )
    .unwrap();

    test_chunked_reader(
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 2, 6, 7],
        &[&[1, 2, 3], &[4, 5, 6], &[7]],
    )
    .unwrap_err();
}

pub struct ChunkedWriter<WriterT> {
    writer: WriterT,
    chunk: Vec<u8>,
    max_chunk_size: usize,
}

impl<WriterT> ChunkedWriter<WriterT> {
    pub fn new(writer: WriterT, max_chunk_size: usize) -> Self {
        Self {
            writer,
            chunk: vec![0; 4],
            max_chunk_size,
        }
    }
}

impl<WriterT: io::Write> ChunkedWriter<WriterT> {
    fn send_chunk(&mut self) -> io::Result<()> {
        let size = (self.chunk.len() - 4).try_into().unwrap();
        (&mut self.chunk[..4]).write_u32::<BigEndian>(size).unwrap();
        self.writer.write_all(&self.chunk)?;
        self.chunk.resize(4, 0);
        Ok(())
    }

    fn remaining_chunk_space(&self) -> usize {
        self.max_chunk_size - (self.chunk.len() - 4)
    }

    pub fn finish(mut self) -> io::Result<()> {
        use std::io::Write as _;

        self.flush()?;
        self.writer.write_u32::<BigEndian>(0)?;
        Ok(())
    }
}

impl<WriterT: io::Write> io::Write for ChunkedWriter<WriterT> {
    fn write(&mut self, mut input: &[u8]) -> io::Result<usize> {
        let to_read = std::cmp::min(self.remaining_chunk_space(), input.len()) as u64;
        let written = std::io::copy(&mut io::Read::take(&mut input, to_read), &mut self.chunk)
            .unwrap() as usize;

        if self.remaining_chunk_space() == 0 {
            self.send_chunk()?;
        }
        if !input.is_empty() {
            return Ok(written + self.write(input)?);
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.chunk.len() > 4 {
            self.send_chunk()?;
        }
        self.writer.flush()
    }
}

#[cfg(test)]
fn test_chunk_writer(input: &[&[u8]], expected: &[u8]) {
    use std::io::Write as _;

    let mut written = vec![];
    let mut writer = ChunkedWriter::new(&mut written, 5);
    for i in input {
        writer.write_all(i).unwrap();
    }

    writer.finish().unwrap();
    assert_eq!(written, expected,);
}

#[test]
fn chunk_writer() {
    test_chunk_writer(
        &[&[1, 2, 3, 4, 5, 6, 7, 8]],
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 3, 6, 7, 8, 0, 0, 0, 0],
    );

    test_chunk_writer(
        &[&[1, 2], &[3, 4], &[5, 6, 7, 8]],
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 3, 6, 7, 8, 0, 0, 0, 0],
    );
    test_chunk_writer(&[&[1, 2]], &[0, 0, 0, 2, 1, 2, 0, 0, 0, 0]);

    test_chunk_writer(
        &[&[1, 2, 3, 4, 5]],
        &[0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 0],
    );
}

#[test]
fn chunk_reader_and_writer() {
    use std::io::{Read as _, Write as _};

    let test_data = Vec::from_iter((0u8..=255).cycle().take(1000));
    let mut encoded = vec![];
    let mut writer = ChunkedWriter::new(&mut encoded, 7);
    writer.write_all(&test_data).unwrap();
    writer.finish().unwrap();

    let mut reader = ChunkedReader::new(&encoded[..]);
    let mut decoded = vec![];
    reader.read_to_end(&mut decoded).unwrap();

    assert_eq!(&decoded, &test_data);
}
