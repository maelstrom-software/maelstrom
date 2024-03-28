//! Useful [`Read`]ers.

use crate::ext::OptionExt as _;
use byteorder::{BigEndian, ReadBytesExt as _, WriteBytesExt as _};
use lru::LruCache;
use maelstrom_base::Sha256Digest;
use sha2::{Digest as _, Sha256};
use std::io::{self, Chain, Read, Repeat, Take, Write};
use std::num::NonZeroUsize;
use std::pin::{pin, Pin};
use std::task::ready;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt as _, AsyncWrite, ReadBuf};

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

#[derive(Default, Debug)]
enum BufferedStreamState {
    #[default]
    Idle,
    Seeking {
        chunk_index: u64,
    },
    Reading {
        chunk_index: u64,
        amount_read: usize,
        new_chunk: Vec<u8>,
    },
}

#[derive(Debug)]
struct BufferedStreamChunk {
    data: Vec<u8>,
    #[allow(dead_code)]
    dirty_start: usize,
}

impl BufferedStreamChunk {
    fn new(data: Vec<u8>) -> Self {
        Self {
            dirty_start: data.len(),
            data,
        }
    }

    fn is_dirty(&self) -> bool {
        self.dirty_start < self.data.len()
    }

    fn dirty(&self) -> &[u8] {
        &self.data[self.dirty_start..]
    }
}

pub struct BufferedStream<StreamT> {
    chunk_size: usize,
    chunks: LruCache<u64, BufferedStreamChunk>,
    position: u64,
    inner_position: u64,
    stream: StreamT,
    state: BufferedStreamState,
    length: u64,
}

impl<StreamT: AsyncSeek + Unpin> BufferedStream<StreamT> {
    pub async fn new(
        chunk_size: usize,
        capacity: NonZeroUsize,
        mut stream: StreamT,
    ) -> io::Result<Self> {
        stream.seek(std::io::SeekFrom::End(0)).await?;
        let length = stream.stream_position().await?;
        stream.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(Self {
            chunk_size,
            chunks: LruCache::new(capacity),
            position: 0,
            inner_position: 0,
            stream,
            state: Default::default(),
            length,
        })
    }
}

impl<StreamT> BufferedStream<StreamT> {
    pub fn get_ref(&self) -> &StreamT {
        &self.stream
    }
}

impl<StreamT: AsyncRead + AsyncWrite + AsyncSeek + Unpin> BufferedStream<StreamT> {
    fn attempt_cached_read(&mut self, dst: &mut ReadBuf<'_>) -> bool {
        let chunk_index = self.position / self.chunk_size as u64;
        if let Some(entry) = self.chunks.get(&chunk_index) {
            let chunk_offset = self.position as usize % self.chunk_size;
            if chunk_offset >= entry.data.len() {
                return true;
            }
            let end = std::cmp::min(entry.data.len(), chunk_offset + dst.remaining());
            dst.put_slice(&entry.data[chunk_offset..end]);
            self.position += (end - chunk_offset) as u64;
            true
        } else {
            false
        }
    }

    fn fill_cache(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.chunks.len() == self.chunks.cap().into() {
            ready!(self.flush_lru(cx))?;
        }

        let chunk_start = (self.position / self.chunk_size as u64) * self.chunk_size as u64;
        let chunk_index = self.position / self.chunk_size as u64;

        if !matches!(&self.state, BufferedStreamState::Reading { .. })
            && self.inner_position != chunk_start
        {
            ready!(self.seek_inner(cx, chunk_index))?;
            assert!(self.is_idle());
        }

        loop {
            match &mut self.state {
                BufferedStreamState::Idle => {
                    self.state = BufferedStreamState::Reading {
                        chunk_index,
                        amount_read: 0,
                        new_chunk: vec![0; self.chunk_size],
                    };
                }
                BufferedStreamState::Reading {
                    chunk_index: existing,
                    amount_read,
                    new_chunk,
                } => {
                    assert_eq!(*existing, chunk_index);
                    let mut buf = ReadBuf::new(&mut new_chunk[*amount_read..]);
                    ready!(AsyncRead::poll_read(pin!(&mut self.stream), cx, &mut buf))?;
                    let just_read = buf.filled().len();
                    self.inner_position += just_read as u64;
                    *amount_read += just_read;
                    if *amount_read == self.chunk_size || just_read == 0 {
                        new_chunk.resize(*amount_read, 0);
                        let new_chunk = BufferedStreamChunk::new(std::mem::take(new_chunk));
                        self.chunks.push(chunk_index, new_chunk).assert_is_none();
                        self.state = BufferedStreamState::Idle;
                        return Poll::Ready(Ok(()));
                    } else {
                        self.state = BufferedStreamState::Reading {
                            chunk_index,
                            amount_read: *amount_read,
                            new_chunk: std::mem::take(new_chunk),
                        };
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn is_idle(&self) -> bool {
        matches!(&self.state, BufferedStreamState::Idle)
    }

    fn attempt_write_to_cache(&mut self, buf: &[u8]) -> Option<usize> {
        let chunk_index = self.position / self.chunk_size as u64;
        if let Some(entry) = self.chunks.get_mut(&chunk_index) {
            let chunk_offset = self.position as usize % self.chunk_size;
            let read_end = chunk_offset + buf.len();
            if entry.data.len() < self.chunk_size && read_end > entry.data.len() {
                let extend_by = read_end - entry.data.len();
                entry.data.resize(
                    std::cmp::min(entry.data.len() + extend_by, self.chunk_size),
                    0,
                );
            }
            let end = std::cmp::min(entry.data.len(), read_end);
            let read_size = end - chunk_offset;
            entry.data[chunk_offset..end].clone_from_slice(&buf[..read_size]);
            self.position += read_size as u64;
            if self.position > self.length {
                self.length = self.position;
            }
            entry.dirty_start = 0;
            return Some(read_size);
        }
        None
    }

    fn seek_inner(&mut self, cx: &mut Context<'_>, chunk_index: u64) -> Poll<io::Result<()>> {
        let chunk_start = chunk_index * self.chunk_size as u64;

        loop {
            match &mut self.state {
                BufferedStreamState::Idle => {
                    ready!(pin!(&mut self.stream).poll_complete(cx))?;
                    pin!(&mut self.stream).start_seek(std::io::SeekFrom::Start(chunk_start))?;
                    self.state = BufferedStreamState::Seeking { chunk_index };
                }
                BufferedStreamState::Seeking {
                    chunk_index: existing,
                } => {
                    assert_eq!(*existing, chunk_index);
                    ready!(pin!(&mut self.stream).poll_complete(cx))?;
                    self.inner_position = chunk_start;
                    self.state = BufferedStreamState::Idle;
                    return Poll::Ready(Ok(()));
                }
                _ => unreachable!(),
            }
        }
    }

    fn flush_chunk(&mut self, cx: &mut Context<'_>, chunk_index: u64) -> Poll<io::Result<()>> {
        let chunk = self.chunks.peek_mut(&chunk_index).unwrap();
        if !chunk.is_dirty() {
            return Poll::Ready(Ok(()));
        }

        let pos = chunk_index * self.chunk_size as u64 + chunk.dirty_start as u64;
        if self.inner_position != pos {
            ready!(self.seek_inner(cx, chunk_index))?;
        }

        let chunk = self.chunks.peek_mut(&chunk_index).unwrap();
        let amount_written = ready!(AsyncWrite::poll_write(
            pin!(&mut self.stream),
            cx,
            chunk.dirty()
        ))?;
        self.inner_position += amount_written as u64;
        chunk.dirty_start += amount_written;

        Poll::Ready(Ok(()))
    }

    fn flush_from_front(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut indices: Vec<_> = self.chunks.iter().map(|(i, _)| *i).collect();
        indices.sort();

        for chunk_index in indices {
            ready!(self.flush_chunk(cx, chunk_index))?;
        }
        Poll::Ready(Ok(()))
    }

    fn flush_lru(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let (chunk_index, _) = self.chunks.peek_lru().unwrap();
        let chunk_index = *chunk_index;
        ready!(self.flush_chunk(cx, chunk_index))?;

        self.chunks.pop(&chunk_index).unwrap();
        Poll::Ready(Ok(()))
    }

    pub fn into_inner(self) -> StreamT {
        self.stream
    }
}

impl<StreamT: AsyncRead + AsyncWrite + AsyncSeek + Unpin> AsyncRead for BufferedStream<StreamT> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if me.attempt_cached_read(dst) {
            assert!(me.is_idle());
            return Poll::Ready(Ok(()));
        }

        ready!(me.fill_cache(cx))?;

        assert!(me.attempt_cached_read(dst));

        assert!(me.is_idle());
        Poll::Ready(Ok(()))
    }
}

impl<StreamT: AsyncRead + AsyncWrite + AsyncSeek + Unpin> AsyncWrite for BufferedStream<StreamT> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();

        if let Some(size) = me.attempt_write_to_cache(buf) {
            assert!(me.is_idle());
            return Poll::Ready(Ok(size));
        }

        ready!(me.fill_cache(cx))?;

        assert!(me.is_idle());
        Poll::Ready(Ok(me.attempt_write_to_cache(buf).unwrap()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        ready!(me.flush_from_front(cx))?;
        AsyncWrite::poll_flush(pin!(&mut me.stream), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        unimplemented!()
    }
}

impl<StreamT: Unpin> AsyncSeek for BufferedStream<StreamT> {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> io::Result<()> {
        let me = self.get_mut();

        match position {
            std::io::SeekFrom::Start(pos) => me.position = pos,
            std::io::SeekFrom::End(offset) => {
                me.position = (me.length as i64 + offset) as u64;
            }
            std::io::SeekFrom::Current(offset) => {
                me.position = (me.position as i64 + offset) as u64;
            }
        }
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let me = self.get_mut();
        Poll::Ready(Ok(me.position))
    }
}

#[tokio::test]
async fn buffered_read() {
    use tokio::io::AsyncReadExt as _;

    for chunk_size in 1..10 {
        let underlying: Vec<_> = (0..10).collect();
        let mut stream = BufferedStream::new(
            chunk_size,
            10.try_into().unwrap(),
            std::io::Cursor::new(underlying),
        )
        .await
        .unwrap();

        let mut buf = [0; 5];

        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf[..], &[0, 1, 2, 3, 4]);

        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf[..], &[5, 6, 7, 8, 9]);
    }
}

#[tokio::test]
async fn buffered_read_cached() {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

    let tmp = tempfile::tempdir().unwrap();
    let fs = crate::async_fs::Fs::new();
    let mut f1 = fs
        .create_file_read_write(tmp.path().join("foo"))
        .await
        .unwrap();

    let mut f2 = f1.try_clone().await.unwrap();
    f2.write_all(&(0..10).collect::<Vec<_>>()).await.unwrap();
    f2.flush().await.unwrap();

    f1.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    let mut stream = BufferedStream::new(4, 10.try_into().unwrap(), f1)
        .await
        .unwrap();

    let mut buf = [0; 3];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], &[0, 1, 2]);

    // Write zeros to the underlying stream
    f2.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    f2.write_all(&[0; 10]).await.unwrap();

    // We should still get the same thing
    stream.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], &[0, 1, 2]);
}

#[tokio::test]
async fn buffered_write_then_read() {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

    for chunk_size in 1..10 {
        let underlying = vec![0; 10];
        let mut stream = BufferedStream::new(
            chunk_size,
            10.try_into().unwrap(),
            std::io::Cursor::new(underlying),
        )
        .await
        .unwrap();

        stream.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
        stream.write_all(&[5, 6, 7, 8, 9]).await.unwrap();
        stream.seek(std::io::SeekFrom::Start(0)).await.unwrap();

        let mut buf = [0; 5];

        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf[..], &[0, 1, 2, 3, 4]);

        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf[..], &[5, 6, 7, 8, 9]);
    }
}

#[tokio::test]
async fn buffered_write_then_flush() {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

    let tmp = tempfile::tempdir().unwrap();
    let fs = crate::async_fs::Fs::new();
    let mut f1 = fs
        .create_file_read_write(tmp.path().join("foo"))
        .await
        .unwrap();

    let mut f2 = f1.try_clone().await.unwrap();

    f1.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    let mut stream = BufferedStream::new(4, 10.try_into().unwrap(), f1)
        .await
        .unwrap();

    stream.write_all(&[0, 1, 2, 3, 4]).await.unwrap();

    // nothing should be written yet
    let file_len = f2.metadata().await.unwrap().len();
    assert_eq!(file_len, 0);

    stream.flush().await.unwrap();

    let mut buf = [0; 5];

    f2.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    f2.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], &[0, 1, 2, 3, 4]);
}

#[cfg(test)]
#[tokio::main]
async fn buffered_stream_simex_test(
    sim: &mut maelstrom_simex::Simulation,
    read_size: usize,
    write_size: usize,
    file_size: usize,
) {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

    let tmp = tempfile::tempdir().unwrap();
    let fs = crate::async_fs::Fs::new();
    let f1 = fs
        .create_file_read_write(tmp.path().join("foo"))
        .await
        .unwrap();

    let mut stream = BufferedStream::new(4, 2.try_into().unwrap(), f1)
        .await
        .unwrap();
    let mut shadow: Vec<_> = (1..(u8::try_from(file_size).unwrap())).collect();

    stream.write_all(&shadow[..]).await.unwrap();
    stream.seek(std::io::SeekFrom::Start(0)).await.unwrap();

    const ITERATIONS: u8 = 5;
    for i in 0..ITERATIONS {
        match sim.choose(["read", "write", "flush", "seek"]).unwrap() {
            "read" => {
                let pos = stream.stream_position().await.unwrap() as usize;
                let read_length = std::cmp::min(read_size, shadow.len() - pos);

                let mut buf = vec![0; read_length];
                stream.read_exact(&mut buf).await.unwrap();

                assert_eq!(&shadow[pos..(pos + read_length)], &buf);
            }
            "write" => {
                let pos = stream.stream_position().await.unwrap() as usize;
                let data = vec![i; write_size];
                stream.write_all(&data[..]).await.unwrap();
                let end = pos + write_size;
                if end > shadow.len() {
                    shadow.resize(end, 0);
                }
                shadow[pos..(pos + write_size)].copy_from_slice(&data[..]);
            }
            "seek" => {
                let pos = ((shadow.len() as u64) / (ITERATIONS - 1) as u64) * i as u64;
                stream.seek(std::io::SeekFrom::Start(pos)).await.unwrap();
            }
            "flush" => {
                stream.flush().await.unwrap();
            }
            _ => unreachable!(),
        }
    }

    stream.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    let mut verify = vec![0; shadow.len()];
    stream.read_exact(&mut verify).await.unwrap();
    assert_eq!(shadow, verify);

    stream.flush().await.unwrap();
    let mut file = stream.into_inner();
    file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    file.read_exact(&mut verify).await.unwrap();

    assert_eq!(shadow, verify);
}

#[test]
fn buffered_stream_simex_read_10_write_15_size_50() {
    maelstrom_simex::SimulationExplorer::default().for_each(|mut sim| {
        buffered_stream_simex_test(&mut sim, 10, 15, 50);
    })
}

#[test]
fn buffered_stream_simex_read_3_write_3_size_10() {
    maelstrom_simex::SimulationExplorer::default().for_each(|mut sim| {
        buffered_stream_simex_test(&mut sim, 3, 3, 10);
    })
}
