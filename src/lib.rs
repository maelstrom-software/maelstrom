use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

pub mod broker;
mod channel_reader;
pub mod client;
mod heap;
mod proto;
pub mod worker;

#[cfg(test)]
pub mod test;

pub type Error = anyhow::Error;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientId(u32);

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientExecutionId(u32);

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ExecutionId(ClientId, ClientExecutionId);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExecutionDetails {
    pub program: String,
    pub arguments: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExecutionResult {
    Exited(u8),
    Signalled(u8),
    Error(String),
}

#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct WorkerId(u32);
