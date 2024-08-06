use anyhow::{bail, Result};
use maelstrom_base::Utf8PathBuf;
use maelstrom_util::async_fs::Fs;
use maelstrom_util::elf::read_shared_libraries;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

fn so_listing_path_from_binary_path(path: &Path) -> PathBuf {
    let mut path = path.to_owned();
    path.set_extension("so_listing");
    path
}

async fn check_for_cached_so_listing(fs: &Fs, binary_path: &Path) -> Result<Option<Vec<PathBuf>>> {
    let listing_path = so_listing_path_from_binary_path(binary_path);
    if fs.exists(&listing_path).await {
        let listing_mtime = fs.metadata(&listing_path).await?.modified()?;
        let binary_mtime = fs.metadata(binary_path).await?.modified()?;
        if binary_mtime < listing_mtime {
            return Ok(Some(decode_paths(fs.open_file(listing_path).await?).await?));
        }
    }
    Ok(None)
}

async fn encode_paths(paths: &[PathBuf], mut out: impl AsyncWrite + Unpin) -> Result<()> {
    out.write_u64(paths.len() as u64).await?;
    for path in paths {
        let s = path.as_os_str();
        out.write_u64(s.len() as u64).await?;
        out.write_all(s.as_encoded_bytes()).await?;
    }
    Ok(())
}

async fn decode_paths(mut input: impl AsyncRead + Unpin) -> Result<Vec<PathBuf>> {
    let mut paths = vec![];
    let num_paths = input.read_u64().await?;
    for _ in 0..num_paths {
        let path_len = input.read_u64().await?;
        let mut buffer = vec![0; path_len as usize];
        input.read_exact(&mut buffer).await?;
        paths.push(OsString::from_vec(buffer).into());
    }

    let extra = tokio::io::copy(&mut input, &mut tokio::io::sink()).await?;
    if extra > 0 {
        bail!("unknown trailing data")
    }

    Ok(paths)
}

pub async fn get_shared_library_dependencies(binary_paths: &[Utf8PathBuf]) -> Result<Vec<PathBuf>> {
    let fs = Fs::new();

    let mut all_paths = BTreeSet::new();
    for binary_path in binary_paths {
        let binary_path = binary_path.as_std_path();
        if let Some(paths) = check_for_cached_so_listing(&fs, binary_path).await? {
            all_paths.extend(paths);
        } else {
            let binary_path_clone = binary_path.to_owned();
            let paths =
                tokio::task::spawn_blocking(move || read_shared_libraries(&binary_path_clone))
                    .await??;
            encode_paths(
                &paths,
                fs.create_file(so_listing_path_from_binary_path(binary_path))
                    .await?,
            )
            .await?;
            all_paths.extend(paths);
        };
    }
    Ok(all_paths.into_iter().collect())
}
