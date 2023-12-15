use meticulous_base::{
    EnumSet, GroupId, JobDevice, JobDeviceListDeserialize, JobMount, JobSpec, NonEmpty,
    Sha256Digest, UserId,
};
use serde::Deserialize;
use std::{io::Read, path::PathBuf};

struct JobSpecIterator<InnerT, LayerMapperT> {
    inner: InnerT,
    layer_mapper: LayerMapperT,
}

impl<InnerT, LayerMapperT> Iterator for JobSpecIterator<InnerT, LayerMapperT>
where
    InnerT: Iterator<Item = serde_json::Result<Job>>,
    LayerMapperT: Fn(String) -> anyhow::Result<NonEmpty<Sha256Digest>>,
{
    type Item = anyhow::Result<JobSpec>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(Err(err)) => Some(Err(anyhow::Error::new(err))),
            Some(Ok(job)) => Some(job.into_job_spec(&self.layer_mapper)),
        }
    }
}

pub fn job_spec_iter_from_reader(
    reader: impl Read,
    layer_mapper: impl Fn(String) -> anyhow::Result<NonEmpty<Sha256Digest>>,
) -> impl Iterator<Item = anyhow::Result<JobSpec>> {
    let inner = serde_json::Deserializer::from_reader(reader).into_iter::<Job>();
    JobSpecIterator {
        inner,
        layer_mapper,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Job {
    program: String,
    arguments: Option<Vec<String>>,
    environment: Option<Vec<String>>,
    layers: NonEmpty<String>,
    devices: Option<EnumSet<JobDeviceListDeserialize>>,
    mounts: Option<Vec<JobMount>>,
    enable_loopback: Option<bool>,
    enable_writable_file_system: Option<bool>,
    working_directory: Option<PathBuf>,
    user: Option<UserId>,
    group: Option<GroupId>,
}

impl Job {
    fn into_job_spec(
        self,
        layer_mapper: impl Fn(String) -> anyhow::Result<NonEmpty<Sha256Digest>>,
    ) -> anyhow::Result<JobSpec> {
        Ok(JobSpec {
            program: self.program,
            arguments: self.arguments.unwrap_or_default(),
            environment: self.environment.unwrap_or_default(),
            layers: NonEmpty::<Sha256Digest>::flatten(self.layers.try_map(layer_mapper)?),
            devices: self
                .devices
                .unwrap_or(EnumSet::EMPTY)
                .into_iter()
                .map(JobDevice::from)
                .collect(),
            mounts: self.mounts.unwrap_or_default(),
            enable_loopback: self.enable_loopback.unwrap_or_default(),
            enable_writable_file_system: self.enable_writable_file_system.unwrap_or_default(),
            working_directory: self.working_directory.unwrap_or_else(|| PathBuf::from("/")),
            user: self.user.unwrap_or(UserId::from(0)),
            group: self.group.unwrap_or(GroupId::from(0)),
        })
    }
}
