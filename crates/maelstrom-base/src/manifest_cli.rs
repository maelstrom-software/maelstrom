use maelstrom_base::manifest::ManifestReader;
use std::{env, io};

fn main() -> io::Result<()> {
    let path = env::args().skip(1).next().ok_or(io::Error::new(
        io::ErrorKind::Other,
        "expected path to manifest",
    ))?;
    let reader = ManifestReader::new(std::fs::File::open(path)?)?;
    for entry in reader {
        let entry = entry?;
        println!("{entry:#?}");
    }

    Ok(())
}
