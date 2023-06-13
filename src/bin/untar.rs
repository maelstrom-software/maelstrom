use clap::Parser;
use meticulous::{Result, Sha256Digest};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Directory the archive is to be extracted into
    #[arg(short, long)]
    output: String,

    /// Expected SHA-256 digest
    #[arg(short, long)]
    checksum: Sha256Digest,

    /// Indicate that the input is gzipped, using tar czf for example
    #[arg(short = 'z', long, default_value_t = false)]
    unzip: bool,
}

struct Sha256Verifier<DelegateT> {
    hasher: Option<sha2::Sha256>,
    delegate: DelegateT,
    expected: Sha256Digest,
}

impl<DelegateT> Sha256Verifier<DelegateT> {
    fn new(delegate: DelegateT, expected: Sha256Digest) -> Self {
        use sha2::Digest;
        Sha256Verifier {
            hasher: Some(sha2::Sha256::new()),
            delegate,
            expected,
        }
    }
}

impl<DelegateT> std::ops::Drop for Sha256Verifier<DelegateT> {
    fn drop(&mut self) {
        assert!(self.hasher.is_none(), "digest never verified");
    }
}

impl<DelegateT: std::io::Read> std::io::Read for Sha256Verifier<DelegateT> {
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
                    if Sha256Digest(hasher.finalize().into()) != self.expected {
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    let input = std::io::stdin();

    if cli.unzip {
        let input = flate2::read::GzDecoder::new(input);
        let mut input = Sha256Verifier::new(input, cli.checksum);
        tar::Archive::new(&mut input).unpack(cli.output)?;
        read_to_end(input)?;
    } else {
        let mut input = Sha256Verifier::new(input, cli.checksum);
        tar::Archive::new(&mut input).unpack(cli.output)?;
        read_to_end(input)?;
    }
    Ok(())
}
