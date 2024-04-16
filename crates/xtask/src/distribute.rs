use anyhow::{anyhow, Result};
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
    Command::new("patchelf").args(args).arg(path).output()?;
    Ok(())
}

#[derive(Debug, Parser)]
pub struct CliArgs {
    binary_path: PathBuf,
}

pub fn main(args: CliArgs) -> Result<()> {
    patchelf(
        &["--set-interpreter", "/lib64/ld-linux-x86-64.so.2"],
        &args.binary_path,
    )?;
    patchelf(&["--remove-rpath"], &args.binary_path)?;
    patchelf(&["--clear-symbol-version", "fmod"], &args.binary_path)?;
    remove_glibc_version_from_version_r(&args.binary_path, "GLIBC_2.38")?;

    Ok(())
}
