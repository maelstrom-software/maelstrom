use anyhow::anyhow;
use meticulous_base::{proto, Sha256Digest};
use meticulous_util::{
    error::Result,
    net::{self, FixedSizeReader},
};
use std::{net::TcpStream, path::PathBuf};

struct Sha256Verifier<'a, DelegateT> {
    hasher: Option<sha2::Sha256>,
    delegate: DelegateT,
    expected: &'a Sha256Digest,
}

impl<'a, DelegateT> Sha256Verifier<'a, DelegateT> {
    fn new(delegate: DelegateT, expected: &'a Sha256Digest) -> Self {
        use sha2::Digest;
        Sha256Verifier {
            hasher: Some(sha2::Sha256::new()),
            delegate,
            expected,
        }
    }
}

impl<'a, DelegateT> std::ops::Drop for Sha256Verifier<'a, DelegateT> {
    fn drop(&mut self) {
        assert!(self.hasher.is_none(), "digest never verified");
    }
}

impl<'a, DelegateT: std::io::Read> std::io::Read for Sha256Verifier<'a, DelegateT> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use sha2::Digest;

        // Take the hasher before we read. If there is an error reading, then we'll leave the
        // struct without a hasher, indicating that it's safe to drop.
        let hasher = self.hasher.take();
        let size = self.delegate.read(buf)?;
        if size > 0 {
            self.hasher = hasher;
            match &mut self.hasher {
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Unexepcted read of non-zero bytes after read of zero bytes or error",
                    ));
                }
                Some(hasher) => {
                    hasher.update(&buf[..size]);
                }
            }
        } else {
            match hasher {
                None => {
                    // We already validated the digest.
                }
                Some(hasher) => {
                    if Sha256Digest(hasher.finalize().into()) != *self.expected {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "SHA-256 digest didn't match",
                        ));
                    }
                }
            }
        }
        Ok(size)
    }
}

fn read_to_end(mut input: impl std::io::Read) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    while input.read(&mut buf)? > 0 {}
    Ok(())
}

pub fn main(
    digest: &Sha256Digest,
    path: PathBuf,
    broker_addr: super::config::Broker,
) -> Result<u64> {
    println!("fetching {digest}");
    let mut writer = TcpStream::connect(broker_addr.inner())?;
    let mut reader = std::io::BufReader::new(writer.try_clone()?);
    println!("writing hello");
    net::write_message_to_socket(&mut writer, proto::Hello::ArtifactFetcher)?;
    println!("writing request");
    net::write_message_to_socket(&mut writer, proto::ArtifactFetcherToBroker(digest.clone()))?;
    println!("reading response");
    match net::read_message_from_socket::<proto::BrokerToArtifactFetcher>(&mut reader)?.0 {
        None => {
            println!("got None");
            Err(anyhow!("Broker error reading artifact {digest}"))
        }
        Some(size) => {
            println!("fetching {digest} of size {size}");
            let mut reader = FixedSizeReader::new(reader, size);
            let mut sha_verifier = Sha256Verifier::new(&mut reader, digest);
            tar::Archive::new(&mut sha_verifier).unpack(path)?;
            println!("tar ended, reading to end");
            read_to_end(sha_verifier)?;
            println!("read to end");
            Ok(size)
        }
    }
}
