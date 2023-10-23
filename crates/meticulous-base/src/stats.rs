//! Contains data-structures for maintaining historical statistics of jobs

pub use crate::ring_buffer::RingBuffer;
use crate::ClientId;
use enum_map::EnumMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, enum_map::Enum, strum::EnumIter, Serialize, Deserialize,
)]
pub enum JobState {
    WaitingForArtifacts,
    Pending,
    Running,
    Complete,
}

impl JobState {
    pub fn iter() -> impl Iterator<Item = Self> {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

/// For a single client, counts of jobs in various states
pub type JobStateCounts = EnumMap<JobState, u64>;

/// Single point-in-time snapshot
/// TODO: This should contain a timestamp
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct JobStatisticsSample {
    pub client_to_stats: HashMap<ClientId, JobStateCounts>,
}

/// The number of data-points to save before it is deleted
pub const CAPACITY: usize = 1024;

/// Time-series of job statistics.
/// It is implemented with a ring buffer. The entries are ordered by time
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct JobStatisticsTimeSeries {
    entries: RingBuffer<JobStatisticsSample>,
}

impl Default for JobStatisticsTimeSeries {
    fn default() -> Self {
        Self {
            entries: RingBuffer::new(CAPACITY),
        }
    }
}

impl FromIterator<JobStatisticsSample> for JobStatisticsTimeSeries {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = JobStatisticsSample>,
    {
        let mut s = Self::default();
        for e in iter {
            s.entries.insert(e);
        }
        s
    }
}

impl JobStatisticsTimeSeries {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, entry: JobStatisticsSample) {
        self.entries.insert(entry);
    }

    pub fn iter(&self) -> impl Iterator<Item = &JobStatisticsSample> {
        self.entries.iter()
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Useful information for a client to display about the broker's state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerStatistics {
    pub num_clients: u64,
    pub num_workers: u64,
    pub job_statistics: JobStatisticsTimeSeries,
}
