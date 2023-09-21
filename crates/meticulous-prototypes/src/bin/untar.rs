use anyhow::Result;
use clap::Parser;
use meticulous_base::Sha256Digest;

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

struct CountingReader<DelegateT> {
    delegate: DelegateT,
    count: u64,
}

impl<DelegateT> CountingReader<DelegateT> {
    fn new(delegate: DelegateT) -> Self {
        CountingReader { delegate, count: 0 }
    }

    fn bytes_read(&self) -> u64 {
        self.count
    }
}

impl<DelegateT: std::io::Read> std::io::Read for CountingReader<DelegateT> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let count = self.delegate.read(buf)?;
        self.count += count as u64;
        Ok(count)
    }
}

fn read_to_end(mut input: impl std::io::Read) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    while input.read(&mut buf)? > 0 {}
    Ok(())
}

fn main2(cli: Cli, input: impl std::io::Read) -> Result<()> {
    let mut counting_reader = CountingReader::new(input);
    let mut sha_verifier = Sha256Verifier::new(&mut counting_reader, cli.checksum);
    tar::Archive::new(&mut sha_verifier).unpack(cli.output)?;
    read_to_end(sha_verifier)?;
    eprintln!("{} bytes read", counting_reader.bytes_read());
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let input = std::io::stdin();
    if cli.unzip {
        main2(cli, flate2::read::GzDecoder::new(input))
    } else {
        main2(cli, input)
    }
}
