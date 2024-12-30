//! This crate contains code that can communicate with GitHub's artifact API.
//! See the documentation in the `client` module for more information.

mod client;
mod queue;

pub use client::*;
pub use queue::*;
