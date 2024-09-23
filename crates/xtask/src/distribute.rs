use anyhow::{anyhow, bail, Result};
use clap::Parser;
use elf::endian::AnyEndian;
use elf::parse::ParseError;
use elf::string_table::StringTable;
use elf::ElfBytes;
use std::collections::BTreeSet;
use std::io::{Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{fmt, mem};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(Clone, Debug, Default, PartialEq, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Verneed {
    version: u16,
    cnt: u16,
    file: u32,
    aux: u32,
    next: u32,
}

#[derive(Clone, Debug, Default, PartialEq, FromZeroes, FromBytes, AsBytes)]
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

#[derive(Clone, Debug, PartialEq)]
struct VerneedEntry {
    need: Verneed,
    aux: Vec<Vernaux>,
}

#[derive(Clone, Debug, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Symbol {
    name: u32,
    info: u8,
    other: u8,
    shndx: u16,
    value: u64,
    size: u64,
}

impl Symbol {
    fn name<'data>(&self, strtab: &'data StringTable) -> Result<&'data str, ParseError> {
        strtab.get(self.name as usize)
    }
}

fn decode_version_entries(mut data: &[u8]) -> Result<Vec<VerneedEntry>> {
    let mut entries = vec![];

    loop {
        let entry_data = &data[..mem::size_of::<Verneed>()];
        let verneed = Verneed::ref_from(entry_data).ok_or_else(|| anyhow!("malformed verneed"))?;
        let mut aux_data = &data[verneed.aux as usize..];
        let mut aux = vec![];
        loop {
            let entry_data = &aux_data[..mem::size_of::<Vernaux>()];
            let vernaux =
                Vernaux::ref_from(entry_data).ok_or_else(|| anyhow!("malformed vernaux"))?;
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

#[test]
fn encode_decode_version_entries() {
    let entries = vec![
        VerneedEntry {
            need: Verneed {
                version: 1,
                file: 12,
                ..Default::default()
            },
            aux: vec![
                Vernaux {
                    hash: 13,
                    flags: 1,
                    other: 2,
                    name: 14,
                    ..Default::default()
                },
                Vernaux {
                    hash: 14,
                    flags: 1,
                    other: 2,
                    name: 15,
                    ..Default::default()
                },
            ],
        },
        VerneedEntry {
            need: Verneed {
                version: 1,
                file: 12,
                ..Default::default()
            },
            aux: vec![Vernaux {
                hash: 15,
                flags: 1,
                other: 2,
                name: 16,
                ..Default::default()
            }],
        },
    ];
    let data = encode_version_entries(entries).unwrap();

    let decoded = decode_version_entries(&data).unwrap();
    assert_eq!(
        decoded,
        vec![
            VerneedEntry {
                need: Verneed {
                    version: 1,
                    cnt: 2,
                    file: 12,
                    aux: mem::size_of::<Verneed>() as u32,
                    next: mem::size_of::<Verneed>() as u32 + mem::size_of::<Vernaux>() as u32 * 2,
                },
                aux: vec![
                    Vernaux {
                        hash: 13,
                        flags: 1,
                        other: 2,
                        name: 14,
                        next: mem::size_of::<Vernaux>() as u32,
                    },
                    Vernaux {
                        hash: 14,
                        flags: 1,
                        other: 2,
                        name: 15,
                        next: 0
                    },
                ],
            },
            VerneedEntry {
                need: Verneed {
                    version: 1,
                    cnt: 1,
                    file: 12,
                    aux: mem::size_of::<Verneed>() as u32,
                    next: 0
                },
                aux: vec![Vernaux {
                    hash: 15,
                    flags: 1,
                    other: 2,
                    name: 16,
                    next: 0
                }],
            },
        ]
    );
}

struct GlibCVersion(versions::Versioning);

impl GlibCVersion {
    fn parse(s: &str) -> Option<Self> {
        let mut iter = s.split('_');
        if iter.next()? != "GLIBC" {
            return None;
        }
        let version = versions::Versioning::new(iter.next()?)?;
        if iter.next().is_some() {
            return None;
        }

        Some(Self(version))
    }

    fn matches(&self, req: &versions::Requirement) -> bool {
        req.matches(&self.0)
    }
}

#[test]
fn glibc_parsing() {
    let v = GlibCVersion::parse("GLIBC_2.38").unwrap();
    assert!(v.matches(&versions::Requirement::new(">=2.38").unwrap()));
    assert!(!v.matches(&versions::Requirement::new("<2.25").unwrap()));

    assert!(GlibCVersion::parse("").is_none());
    assert!(GlibCVersion::parse("FOOBAR_2.38").is_none());
    assert!(GlibCVersion::parse("GLIBC").is_none());
    assert!(GlibCVersion::parse("GLIBC_").is_none());
    assert!(GlibCVersion::parse("GLIBC_3...2").is_none());
    assert!(GlibCVersion::parse("GLIBC_2.38_foo").is_none());
}

fn remove_glibc_versions_from_version_r(
    path: &Path,
    to_remove: versions::Requirement,
    report: &mut BinaryPatchReport,
) -> Result<Vec<u16>> {
    let file_data = std::fs::read(path)?;
    let slice = file_data.as_slice();
    let file = ElfBytes::<AnyEndian>::minimal_parse(slice)?;

    let dynstr = file
        .section_header_by_name(".dynstr")?
        .ok_or_else(|| anyhow!(".dynstr section not found"))?;
    let strtab = file.section_data_as_strtab(&dynstr)?;

    // decode the .gnu.version_r section
    let gnu_version_header = file
        .section_header_by_name(".gnu.version_r")?
        .ok_or_else(|| anyhow!(".gnu.version_r section not found"))?;
    let (data, _) = file.section_data(&gnu_version_header)?;
    let mut entries = decode_version_entries(data)?;

    // Remove the version entry we are interested in
    let mut removed = vec![];
    for entry in &mut entries {
        for aux in mem::take(&mut entry.aux) {
            let name = aux.name(&strtab)?;
            if let Some(v) = GlibCVersion::parse(name) {
                if v.matches(&to_remove) {
                    removed.push(aux.other);
                    report.glibc_versions_removed.insert(name.to_owned());
                    continue;
                }
            }
            entry.aux.push(aux);
        }
    }

    // Encoded the updated entries
    let mut encoded = encode_version_entries(entries)?;

    // Pad it the old section size
    assert!(encoded.len() <= gnu_version_header.sh_size as usize);
    encoded.resize(gnu_version_header.sh_size as usize, 0);

    // Rewrite that section of the file
    let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.seek(std::io::SeekFrom::Start(gnu_version_header.sh_offset))?;
    file.write_all(&encoded)?;

    Ok(removed)
}

fn remove_symbol_versions(
    path: &Path,
    versions_to_remove: Vec<u16>,
    report: &mut BinaryPatchReport,
) -> Result<()> {
    let file_data = std::fs::read(path)?;
    let slice = file_data.as_slice();
    let file = ElfBytes::<AnyEndian>::minimal_parse(slice)?;

    let dynstr = file
        .section_header_by_name(".dynstr")?
        .ok_or_else(|| anyhow!(".dynstr section not found"))?;
    let strtab = file.section_data_as_strtab(&dynstr)?;

    let dynamic_symbols = file.section_header_by_name(".dynsym").unwrap().unwrap();
    let symbol_versions = file
        .section_header_by_name(".gnu.version")
        .unwrap()
        .unwrap();
    let (mut data, _) = file.section_data(&dynamic_symbols).unwrap();
    let (symbol_version_data, _) = file.section_data(&symbol_versions).unwrap();

    let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
    let mut symbol_index = 0;
    while !data.is_empty() {
        let symbol_data = &data[..mem::size_of::<Symbol>()];
        let symbol = Symbol::ref_from(symbol_data).unwrap();
        let version_index = u16::from_le_bytes([
            symbol_version_data[symbol_index * 2],
            symbol_version_data[symbol_index * 2 + 1],
        ]) & 0x7fff; // highest bit is a flag

        if versions_to_remove.contains(&version_index) {
            let offset = symbol_versions.sh_offset + (symbol_index * 2) as u64;
            file.seek(std::io::SeekFrom::Start(offset))?;
            // 1 means a global symbol
            file.write_all(&1u16.to_le_bytes())?;
            report
                .symbols_made_global
                .insert(symbol.name(&strtab)?.to_owned());
        }

        data = &data[mem::size_of::<Symbol>()..];
        symbol_index += 1;
    }
    Ok(())
}

fn patchelf(args: &[&str], path: impl AsRef<Path>) -> Result<String> {
    let output = Command::new("patchelf")
        .args(args)
        .arg(path.as_ref())
        .output()?;
    if !output.status.success() {
        bail!("pathelf failed");
    }
    Ok(String::from_utf8(output.stdout).unwrap())
}

#[derive(Default)]
struct BinaryPatchReport {
    glibc_versions_removed: BTreeSet<String>,
    symbols_made_global: BTreeSet<String>,
}

impl fmt::Display for BinaryPatchReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "glibc versions removed: {:?}",
            &self.glibc_versions_removed
        )?;
        write!(f, "symbols made global: {:?}", &self.symbols_made_global)?;
        Ok(())
    }
}

fn patch_binary(path: &Path, report: &mut BinaryPatchReport) -> Result<()> {
    // I'm not sure the best way to get this value, here I am copying it from the system ls binary
    let interpreter_str = patchelf(&["--print-interpreter"], "/bin/ls")?
        .trim()
        .to_owned();

    // Remove the nix specific stuff
    patchelf(&["--set-interpreter", &interpreter_str], path)?;
    patchelf(&["--remove-rpath"], path)?;

    // The highest version of glibc for Ubuntu 22.04 is 2.35
    let req = versions::Requirement::new(">2.35").unwrap();

    // Remove any requirement on high versions of glibc
    let removed_versions = remove_glibc_versions_from_version_r(path, req, report)?;
    remove_symbol_versions(path, removed_versions, report)?;

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

const ARTIFACT_NAMES: [&str; 7] = [
    "cargo-maelstrom",
    "maelstrom-broker",
    "maelstrom-client",
    "maelstrom-go-test",
    "maelstrom-pytest",
    "maelstrom-run",
    "maelstrom-worker",
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

fn package_artifacts(
    temp_dir: &tempfile::TempDir,
    target_triple: &str,
    binaries: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let mut packaged = vec![];
    let mut report = BinaryPatchReport::default();
    for binary_path in binaries {
        let new_binary = temp_dir.path().join(binary_path.file_name().unwrap());
        std::fs::copy(binary_path, &new_binary)?;
        patch_binary(&new_binary, &mut report)?;
        let tar_gz_path = temp_dir.path().join(format!(
            "{}-{target_triple}.tgz",
            new_binary.file_name().unwrap().to_str().unwrap()
        ));
        tar_gz(&new_binary, &tar_gz_path)?;
        packaged.push(tar_gz_path)
    }
    println!("{report}");
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

fn get_target_triple() -> Result<String> {
    let output = Command::new("rustc").arg("-vV").output()?;
    if !output.status.success() {
        bail!("rustc -vV failed");
    }
    let stdout = String::from_utf8(output.stdout).unwrap();
    for line in stdout.split('\n').skip(1) {
        let mut split = line.split(':');
        let key = split.next().unwrap();
        let value = split.next().unwrap();
        if key == "host" {
            return Ok(value.trim().into());
        }
    }
    bail!("failed to find \"host\" in rustc -vV output");
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

    let target_triple = get_target_triple()?;
    let packaged = package_artifacts(&temp_dir, &target_triple, &binary_paths)?;
    upload(&packaged, &tag, args.dry_run)?;
    Ok(())
}
