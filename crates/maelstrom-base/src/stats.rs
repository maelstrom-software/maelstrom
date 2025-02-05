//! Contains data-structures for maintaining historical statistics of jobs

use crate::{
    ring_buffer::{RingBuffer, RingBufferIter},
    ClientId, WorkerId,
};
use enum_map::EnumMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, time::Duration};

/// The amount of time between broker statistic samples
pub const BROKER_STATISTICS_INTERVAL: Duration = Duration::from_millis(500);

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    enum_map::Enum,
    strum::EnumIter,
    Serialize,
    Deserialize,
    Hash,
    PartialOrd,
    Ord,
)]
pub enum JobState {
    WaitingForArtifacts,
    Pending,
    Running,
    Complete,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WaitingForArtifacts => write!(f, "waiting for artifacts"),
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Complete => write!(f, "complete"),
        }
    }
}

impl JobState {
    pub fn iter() -> impl DoubleEndedIterator<Item = Self> {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

#[test]
fn job_state_iter() {
    let exp = vec![
        JobState::WaitingForArtifacts,
        JobState::Pending,
        JobState::Running,
        JobState::Complete,
    ];
    assert_eq!(Vec::from_iter(JobState::iter()), exp);
    assert_eq!(
        Vec::from_iter(JobState::iter().rev()),
        Vec::from_iter(exp.into_iter().rev())
    );
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
#[derive(Serialize, Default, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct JobStatisticsTimeSeries {
    entries: RingBuffer<JobStatisticsSample, CAPACITY>,
}

impl FromIterator<JobStatisticsSample> for JobStatisticsTimeSeries {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = JobStatisticsSample>,
    {
        let mut s = Self::default();
        for e in iter {
            s.entries.push(e);
        }
        s
    }
}

impl JobStatisticsTimeSeries {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, entry: JobStatisticsSample) {
        self.entries.push(entry);
    }

    pub fn iter(&self) -> RingBufferIter<'_, JobStatisticsSample, CAPACITY> {
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkerStatistics {
    pub slots: usize,
}

/// Useful information for a client to display about the broker's state.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct BrokerStatistics {
    pub worker_statistics: HashMap<WorkerId, WorkerStatistics>,
    pub job_statistics: JobStatisticsTimeSeries,
}
