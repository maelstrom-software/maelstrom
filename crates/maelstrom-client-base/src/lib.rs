mod proto_buf_conv;
pub mod spec;

pub mod proto {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/maelstrom_client_base.items.rs"));
}

// This hack makes some macros in maelstrom_test work correctly
#[cfg(test)]
extern crate self as maelstrom_client;

pub use proto_buf_conv::{IntoProtoBuf, TryFromProtoBuf};

use anyhow::{anyhow, Result};
use enum_map::EnumMap;
use maelstrom_macro::{IntoProtoBuf, TryFromProtoBuf};

/// The project directory is used for two things. First, any relative paths in layer specifications
/// are resolved based on this path. Second, it's where the client process looks for the
/// `maelstrom-container-tags.lock` file.
pub struct ProjectDir;

/// According to the XDG base directories spec:
///
///   The state directory contains state data that should persist between (application) restarts,
///   but that is not important or portable enough to the user that it should be stored in
///   $XDG_DATA_HOME. It may contain:
///     - actions history (logs, history, recently used files, ...)
///     - current state of the application that can be reused on a restart (view, layout, open
///       files, undo history, ...)
///
/// For the client process, that currently just means the log files.
pub struct StateDir;

/// The cache directory is where we put a variety of different caches. The local worker's cache
/// directory lives inside of this client cache directory. Another cache in this directory is the
/// manifest cache.
pub struct CacheDir;

pub const MANIFEST_DIR: &str = "manifests";
pub const STUB_MANIFEST_DIR: &str = "manifests/stubs";
pub const SYMLINK_MANIFEST_DIR: &str = "manifests/symlinks";

impl From<proto::Error> for anyhow::Error {
    fn from(e: proto::Error) -> Self {
        anyhow::Error::msg(e.message)
    }
}

pub trait IntoResult {
    type Output;
    fn into_result(self) -> Result<Self::Output>;
}

impl IntoResult for proto::Void {
    type Output = ();

    fn into_result(self) -> Result<()> {
        Ok(())
    }
}

impl<V> IntoResult for anyhow::Result<V> {
    type Output = V;

    fn into_result(self) -> Result<Self::Output> {
        self
    }
}

impl<V> IntoResult for Option<V> {
    type Output = V;

    fn into_result(self) -> Result<Self::Output> {
        match self {
            Some(res) => Ok(res),
            None => Err(anyhow!("malformed response")),
        }
    }
}

impl<V> IntoResult for Vec<V> {
    type Output = Vec<V>;

    fn into_result(self) -> Result<Self::Output> {
        Ok(self)
    }
}

#[derive(IntoProtoBuf, TryFromProtoBuf)]
#[proto(other_type = "proto::RemoteProgress")]
pub struct RemoteProgress {
    pub name: String,
    pub size: u64,
    pub progress: u64,
}

#[derive(IntoProtoBuf, TryFromProtoBuf)]
#[proto(other_type = "proto::IntrospectResponse")]
pub struct IntrospectResponse {
    #[proto(option)]
    pub job_state_counts: EnumMap<maelstrom_base::stats::JobState, u64>,
    pub artifact_uploads: Vec<RemoteProgress>,
    pub image_downloads: Vec<RemoteProgress>,
}
