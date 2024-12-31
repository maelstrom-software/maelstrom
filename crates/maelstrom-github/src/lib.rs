//! This crate contains code that can communicate with GitHub's artifact API.
//! See the documentation in the `client` module for more information.

mod client;
mod queue;

use chrono::{DateTime, Utc};
use std::time::Duration;

pub use client::*;
pub use queue::*;

fn two_hours_from_now() -> DateTime<Utc> {
    Utc::now() + Duration::from_secs(60 * 60 * 24 * 2)
}
