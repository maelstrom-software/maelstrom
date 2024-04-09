mod proto_buf_conv;
pub mod spec;

pub mod proto {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/maelstrom_client_base.items.rs"));
}

// This hack makes some macros in maelstrom_test work correctly
#[cfg(test)]
extern crate self as maelstrom_client;

use anyhow::{anyhow, Result};
use maelstrom_macro::{IntoProtoBuf, TryFromProtoBuf};
pub use proto_buf_conv::{IntoProtoBuf, TryFromProtoBuf};
use serde::{Deserialize, Serialize};

pub const MANIFEST_DIR: &str = "maelstrom-manifests";
pub const STUB_MANIFEST_DIR: &str = "maelstrom-manifests/stubs";
pub const SYMLINK_MANIFEST_DIR: &str = "maelstrom-manifests/symlinks";

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

#[derive(IntoProtoBuf, TryFromProtoBuf, Default, Debug, Serialize, Deserialize)]
#[proto(other_type = "proto::ClientDriverMode")]
pub enum ClientDriverMode {
    #[default]
    MultiThreaded,
    SingleThreaded,
}

#[derive(IntoProtoBuf, TryFromProtoBuf, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[proto(other_type = "proto::ClientMessageKind")]
pub enum ClientMessageKind {
    AddJob,
    GetJobStateCounts,
    Stop,
    Other,
}

#[derive(IntoProtoBuf, TryFromProtoBuf)]
#[proto(other_type = "proto::ArtifactUploadProgress")]
pub struct ArtifactUploadProgress {
    pub name: String,
    pub size: u64,
    pub progress: u64,
}
