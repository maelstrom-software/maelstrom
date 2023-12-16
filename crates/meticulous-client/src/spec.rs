use meticulous_base::{
    EnumSet, GroupId, JobDevice, JobDeviceListDeserialize, JobMount, JobSpec, NonEmpty,
    Sha256Digest, UserId,
};
use serde::{de, Deserialize, Deserializer};
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

#[derive(Debug, Eq, PartialEq)]
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
    #[cfg(test)]
    fn new(program: String, layers: NonEmpty<String>) -> Self {
        Job {
            program,
            layers,
            arguments: None,
            environment: None,
            devices: None,
            mounts: None,
            enable_loopback: None,
            enable_writable_file_system: None,
            working_directory: None,
            user: None,
            group: None,
        }
    }

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

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum JobField {
    Program,
    Arguments,
    Environment,
    Layers,
    Devices,
    Mounts,
    EnableLoopback,
    EnableWritableFileSystem,
    WorkingDirectory,
    User,
    Group,
}

struct JobVisitor;

impl<'de> de::Visitor<'de> for JobVisitor {
    type Value = Job;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Job")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut program = None;
        let mut arguments = None;
        let mut environment = None;
        let mut layers = None;
        let mut devices = None;
        let mut mounts = None;
        let mut enable_loopback = None;
        let mut enable_writable_file_system = None;
        let mut working_directory = None;
        let mut user = None;
        let mut group = None;
        while let Some(key) = map.next_key()? {
            match key {
                JobField::Program => {
                    program = Some(map.next_value()?);
                }
                JobField::Arguments => {
                    arguments = Some(map.next_value()?);
                }
                JobField::Environment => {
                    environment = Some(map.next_value()?);
                }
                JobField::Layers => {
                    layers = Some(map.next_value()?);
                }
                JobField::Devices => {
                    devices = Some(map.next_value()?);
                }
                JobField::Mounts => {
                    mounts = Some(map.next_value()?);
                }
                JobField::EnableLoopback => {
                    enable_loopback = Some(map.next_value()?);
                }
                JobField::EnableWritableFileSystem => {
                    enable_writable_file_system = Some(map.next_value()?);
                }
                JobField::WorkingDirectory => {
                    working_directory = Some(map.next_value()?);
                }
                JobField::User => {
                    user = Some(map.next_value()?);
                }
                JobField::Group => {
                    group = Some(map.next_value()?);
                }
            }
        }
        Ok(Job {
            program: program.ok_or_else(|| de::Error::missing_field("program"))?,
            arguments,
            environment,
            layers: layers.ok_or_else(|| de::Error::missing_field("layers"))?,
            devices,
            mounts,
            enable_loopback,
            enable_writable_file_system,
            working_directory,
            user,
            group,
        })
    }
}

impl<'de> de::Deserialize<'de> for Job {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JobVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use meticulous_base::{enum_set, nonempty, JobMountFsType};
    use meticulous_test::digest;
    use serde_json::{json, Value};

    fn layer_mapper(layer: String) -> anyhow::Result<NonEmpty<Sha256Digest>> {
        Ok(nonempty![Sha256Digest::from(layer.parse::<u64>()?)])
    }

    #[test]
    fn minimum_into_job_spec() {
        assert_eq!(
            Job::new("program".to_string(), nonempty!["1".to_string()])
                .into_job_spec(layer_mapper)
                .unwrap(),
            JobSpec::new("program", nonempty![digest!(1)]),
        );
    }

    #[test]
    fn most_into_job_spec() {
        assert_eq!(
            Job {
                arguments: Some(vec!["arg1".to_string(), "arg2".to_string()]),
                environment: Some(vec!["FOO=foo".to_string(), "BAR=bar".to_string()]),
                devices: Some(enum_set! {JobDeviceListDeserialize::Null}),
                mounts: Some(vec![JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: "/tmp".into()
                }]),
                working_directory: Some("/working-directory".into()),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
                ..Job::new("program".to_string(), nonempty!["1".to_string()])
            }
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("program", nonempty![digest!(1)])
                .arguments(["arg1", "arg2"])
                .environment(["FOO=foo", "BAR=bar"])
                .devices(enum_set! {JobDevice::Null})
                .mounts([JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: "/tmp".into()
                }])
                .working_directory("/working-directory")
                .user(101)
                .group(202),
        );
    }

    #[test]
    fn enable_loopback_into_job_spec() {
        assert_eq!(
            Job {
                enable_loopback: Some(true),
                ..Job::new("program".to_string(), nonempty!["1".to_string()])
            }
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("program", nonempty![digest!(1)]).enable_loopback(true),
        );
    }

    #[test]
    fn enable_writable_file_system_into_job_spec() {
        assert_eq!(
            Job {
                enable_writable_file_system: Some(true),
                ..Job::new("program".to_string(), nonempty!["1".to_string()])
            }
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("program", nonempty![digest!(1)]).enable_writable_file_system(true),
        );
    }

    fn parse_job(value: Value) -> serde_json::Result<Job> {
        serde_json::from_value(value)
    }

    fn assert_error(err: serde_json::Error, expected: &str) {
        let message = format!("{err}");
        assert!(message.starts_with(expected), "message: {message}");
    }

    #[test]
    fn basic() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]),
        );
    }

    #[test]
    fn missing_program() {
        assert_error(
            parse_job(json!({
                "layers": [ "1" ],
            }))
            .unwrap_err(),
            "missing field `program`",
        );
    }

    #[test]
    fn missing_layers() {
        assert_error(
            parse_job(json!({
                "program": "/bin/sh",
            }))
            .unwrap_err(),
            "missing field `layers`",
        );
    }

    #[test]
    fn arguments() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "arguments": [ "-e", "echo foo" ],
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)])
                .arguments(["-e", "echo foo"]),
        )
    }

    #[test]
    fn environment() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "environment": [ "FOO=foo", "BAR=bar" ],
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)])
                .environment(["FOO=foo", "BAR=bar"]),
        )
    }

    #[test]
    fn devices() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "devices": [ "null", "zero" ],
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)])
                .devices(enum_set! {JobDevice::Null | JobDevice::Zero}),
        )
    }

    #[test]
    fn mounts() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "mounts": [
                    { "fs_type": "tmp", "mount_point": "/tmp" },
                ],
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]).mounts([JobMount {
                fs_type: JobMountFsType::Tmp,
                mount_point: "/tmp".to_string()
            }])
        )
    }

    #[test]
    fn enable_loopback() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "enable_loopback": true,
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]).enable_loopback(true),
        )
    }

    #[test]
    fn enable_writable_file_system() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "enable_writable_file_system": true,
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]).enable_writable_file_system(true),
        )
    }

    #[test]
    fn working_directory() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "working_directory": "/foo/bar",
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]).working_directory("/foo/bar"),
        )
    }

    #[test]
    fn user() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "user": 1234,
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]).user(1234),
        )
    }

    #[test]
    fn group() {
        assert_eq!(
            parse_job(json!({
                "program": "/bin/sh",
                "layers": [ "1" ],
                "group": 4321,
            }))
            .unwrap()
            .into_job_spec(layer_mapper)
            .unwrap(),
            JobSpec::new("/bin/sh".to_string(), nonempty![digest!(1)]).group(4321),
        )
    }
}
