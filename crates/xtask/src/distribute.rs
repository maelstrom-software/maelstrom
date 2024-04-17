use anyhow::{anyhow, bail, Result};
use clap::Parser;
use elf::endian::AnyEndian;
use elf::parse::ParseError;
use elf::string_table::StringTable;
use elf::ElfBytes;
use std::io::{Seek as _, Write as _};
use std::mem;
use std::path::{Path, PathBuf};
use std::process::Command;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(Clone, Debug, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Verneed {
    version: u16,
    cnt: u16,
    file: u32,
    aux: u32,
    next: u32,
}

#[derive(Clone, Debug, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Vernaux {
    hash: u32,
    flags: u16,
    other: u16,
    name: u32,
    next: u32,
}

impl Vernaux {
    fn name<'data>(&self, strtab: &'data StringTable) -> Result<&'data str, ParseError> {
        strtab.get(self.name as usize)
    }
}

#[derive(Clone, Debug)]
struct VerneedEntry {
    need: Verneed,
    aux: Vec<Vernaux>,
}

fn decode_version_entries(mut data: &[u8]) -> Result<Vec<VerneedEntry>> {
    let mut entries = vec![];

    loop {
        let entry_data = &data[..mem::size_of::<Verneed>()];
        let verneed = Verneed::ref_from(entry_data).ok_or(anyhow!("malformed verneed"))?;
        let mut aux_data = &data[verneed.aux as usize..];
        let mut aux = vec![];
        loop {
            let entry_data = &aux_data[..mem::size_of::<Vernaux>()];
            let vernaux = Vernaux::ref_from(entry_data).ok_or(anyhow!("malformed vernaux"))?;
            aux.push(vernaux.clone());
            if vernaux.next == 0 {
                break;
            }
            aux_data = &aux_data[vernaux.next as usize..];
        }
        entries.push(VerneedEntry {
            need: verneed.clone(),
            aux,
        });

        if verneed.next == 0 {
            break;
        }
        data = &data[verneed.next as usize..];
    }

    Ok(entries)
}

fn encode_version_entries(entries: Vec<VerneedEntry>) -> Result<Vec<u8>> {
    let mut encoded = vec![];
    let num_entries = entries.len();
    for (i, mut entry) in entries.into_iter().enumerate() {
        let num_aux = entry.aux.len();

        entry.need.aux = mem::size_of::<Verneed>() as u32;
        entry.need.cnt = entry.aux.len() as u16;
        if i == num_entries - 1 {
            entry.need.next = 0;
        } else {
            entry.need.next = entry.need.aux + mem::size_of::<Vernaux>() as u32 * num_aux as u32;
        }
        encoded.extend(entry.need.as_bytes());
        for (i, mut aux) in entry.aux.into_iter().enumerate() {
            if i == num_aux - 1 {
                aux.next = 0;
            } else {
                aux.next = mem::size_of::<Vernaux>() as u32;
            }
            encoded.extend(aux.as_bytes());
        }
    }

    Ok(encoded)
}

fn remove_glibc_version_from_version_r(path: &Path, version: &str) -> Result<()> {
    let file_data = std::fs::read(path)?;
    let slice = file_data.as_slice();
    let file = ElfBytes::<AnyEndian>::minimal_parse(slice)?;

    let dynstr = file
        .section_header_by_name(".dynstr")?
        .ok_or(anyhow!(".dynstr section not found"))?;
    let strtab = file.section_data_as_strtab(&dynstr)?;

    // decode the .gnu.version_r section
    let gnu_version_header = file
        .section_header_by_name(".gnu.version_r")?
        .ok_or(anyhow!(".gnu.version_r section not found"))?;
    let (data, _) = file.section_data(&gnu_version_header)?;
    let mut entries = decode_version_entries(data)?;

    // Remove the version entry we are interested in
    for entry in &mut entries {
        for aux in mem::take(&mut entry.aux) {
            if aux.name(&strtab)? != version {
                entry.aux.push(aux);
            }
        }
    }

    // Encoded the updated entries
    let mut encoded = encode_version_entries(entries)?;

    // Pad it the old section size
    assert!(encoded.len() <= gnu_version_header.sh_size as usize);
    encoded.resize(gnu_version_header.sh_size as usize, 0);

    // Rewrite that section of the file
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(std::io::SeekFrom::Start(gnu_version_header.sh_offset))
        .unwrap();
    file.write_all(&encoded).unwrap();

    Ok(())
}

fn patchelf(args: &[&str], path: &Path) -> Result<()> {
    if !Command::new("patchelf")
        .args(args)
        .arg(path)
        .status()?
        .success()
    {
        bail!("pathelf failed");
    }
    Ok(())
}

fn patch_binary(path: &Path) -> Result<()> {
    patchelf(&["--set-interpreter", "/lib64/ld-linux-x86-64.so.2"], path)?;
    patchelf(&["--remove-rpath"], path)?;
    patchelf(&["--clear-symbol-version", "fmod"], path)?;
    remove_glibc_version_from_version_r(path, "GLIBC_2.38")?;
    Ok(())
}

/// Package and upload artifacts to github.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Version to add artifacts to
    version: String,
    /// Just print the upload command instead of actually uploading
    #[clap(long)]
    dry_run: bool,
}

const ARTIFACT_NAMES: [&str; 4] = [
    "cargo-maelstrom",
    "maelstrom-worker",
    "maelstrom-broker",
    "maelstrom-run",
];

fn tar_gz(binary: &Path, target: &Path) -> Result<()> {
    let mut cmd = Command::new("tar");
    cmd.arg("cfz").arg(target);
    if let Some(parent) = binary.parent() {
        cmd.arg("-C").arg(parent);
    }
    cmd.arg(binary.file_name().unwrap());
    if !cmd.status()?.success() {
        bail!("tar cfz failed");
    }
    Ok(())
}

fn get_binary_paths() -> Result<Vec<PathBuf>> {
    let mut paths = vec![];
    for a in ARTIFACT_NAMES {
        let binary_path = PathBuf::from("target/release").join(a);
        if !binary_path.exists() {
            bail!("{} does not exist", binary_path.display());
        }
        paths.push(binary_path);
    }
    Ok(paths)
}

fn package_artifacts(temp_dir: &tempfile::TempDir, binaries: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut packaged = vec![];
    for binary_path in binaries {
        let new_binary = temp_dir.path().join(binary_path.file_name().unwrap());
        std::fs::copy(binary_path, &new_binary)?;
        patch_binary(&new_binary)?;
        let tar_gz_path = new_binary.with_extension("tar.gz");
        tar_gz(&new_binary, &tar_gz_path)?;
        packaged.push(tar_gz_path)
    }
    Ok(packaged)
}

fn prompt(msg: &str, yes: &str, no: &str) -> Result<bool> {
    loop {
        print!("{}", msg);
        std::io::stdout().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        if line.trim() == yes {
            return Ok(true);
        }
        if line.trim() == no {
            return Ok(false);
        }
    }
}

fn upload(paths: &[PathBuf], tag: &str, dry_run: bool) -> Result<()> {
    let mut cmd = Command::new("gh");
    cmd.arg("release").arg("upload").arg(tag).args(paths);
    if !dry_run {
        if !cmd.status()?.success() {
            bail!("gh release failed");
        }
    } else {
        println!("dry-run, command to run:");
        println!("{cmd:?}");
    }
    Ok(())
}

pub fn main(args: CliArgs) -> Result<()> {
    let tag = args.version;
    let temp_dir = tempfile::tempdir()?;
    let binary_paths = get_binary_paths()?;
    println!("Package and upload the following binaries for {tag}?");
    for p in &binary_paths {
        println!("    {}", p.display());
    }
    println!();
    if !prompt("yes or no? ", "yes", "no")? {
        return Ok(());
    }

    let packaged = package_artifacts(&temp_dir, &binary_paths)?;
    upload(&packaged, &tag, args.dry_run)?;
    Ok(())
}
