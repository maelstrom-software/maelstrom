use anyhow::{bail, Result};
use maelstrom_base::{Sha256Digest, Utf8PathBuf};
use maelstrom_util::async_fs::Fs;
use maelstrom_util::elf::read_shared_libraries;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

fn so_listing_path_from_binary_path(so_listings_path: &Path, digest: &Sha256Digest) -> PathBuf {
    so_listings_path.join(digest.to_string())
}

async fn check_for_cached_so_listing(
    fs: &Fs,
    so_listings_path: &Path,
    binary_digest: &Sha256Digest,
) -> Result<Option<Vec<PathBuf>>> {
    let listing_path = so_listing_path_from_binary_path(so_listings_path, binary_digest);
    if fs.exists(&listing_path).await {
        Ok(Some(decode_paths(fs.open_file(listing_path).await?).await?))
    } else {
        Ok(None)
    }
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

pub async fn get_shared_library_dependencies(
    project_dir: &Path,
    so_listings_path: &Path,
    data_upload: &mut impl super::DataUpload,
    binary_paths: &[Utf8PathBuf],
) -> Result<Vec<PathBuf>> {
    let fs = Fs::new();

    fs.create_dir_all(so_listings_path).await?;

    let mut all_paths = BTreeSet::new();
    for binary_path in binary_paths {
        let binary_path = project_dir.join(binary_path.as_std_path());

        // Even though we don't include the binary in the manifest we create, we use this to get a
        // cached digest of the contents for caching.
        let binary_digest = data_upload.upload(&binary_path).await?;

        if let Some(paths) =
            check_for_cached_so_listing(&fs, so_listings_path, &binary_digest).await?
        {
            all_paths.extend(paths);
        } else {
            let binary_path_clone = binary_path.clone();
            let paths =
                tokio::task::spawn_blocking(move || read_shared_libraries(&binary_path_clone))
                    .await??;
            encode_paths(
                &paths,
                fs.create_file(so_listing_path_from_binary_path(
                    so_listings_path,
                    &binary_digest,
                ))
                .await?,
            )
            .await?;
            all_paths.extend(paths);
        };
    }
    Ok(all_paths.into_iter().collect())
}
