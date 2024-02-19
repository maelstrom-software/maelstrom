pub mod comm;
pub mod spec;

// This hack makes some macros in maelstrom_base work correctly
#[cfg(test)]
extern crate self as maelstrom_client;

use anyhow::Result;
use maelstrom_base::{ClientJobId, JobOutcomeResult};
use serde::{Deserialize, Serialize};
use std::os::linux::net::SocketAddrExt as _;
use std::os::unix::net::{SocketAddr, UnixListener, UnixStream};

pub type JobResponseHandler = Box<dyn FnOnce(ClientJobId, JobOutcomeResult) + Send + Sync>;

pub const MANIFEST_DIR: &str = "maelstrom-manifests";
pub const STUB_MANIFEST_DIR: &str = "maelstrom-manifests/stubs";
pub const SYMLINK_MANIFEST_DIR: &str = "maelstrom-manifests/symlinks";

#[derive(Default, Debug, Serialize, Deserialize)]
pub enum ClientDriverMode {
    #[default]
    MultiThreaded,
    SingleThreaded,
}

fn address_from_pid(id: u32) -> Result<SocketAddr> {
    let mut name = b"maelstrom-client".to_vec();
    name.extend(id.to_be_bytes());
    Ok(SocketAddr::from_abstract_name(name)?)
}

pub fn listen(id: u32) -> Result<UnixListener> {
    Ok(UnixListener::bind_addr(&address_from_pid(id)?)?)
}

pub fn connect(id: u32) -> Result<UnixStream> {
    let mut name = b"maelstrom-client".to_vec();
    name.extend(id.to_be_bytes());
    Ok(UnixStream::connect_addr(&address_from_pid(id)?)?)
}
