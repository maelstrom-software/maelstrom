use anyhow::{bail, Result};
use maelstrom_base::{
    EnumSet, GroupId, JobMountForTomlAndJson, JobNetwork, JobRootOverlay, NonEmpty, Timeout,
    UserId, Utf8PathBuf,
};
use maelstrom_client::spec::{
    incompatible, ContainerParent, ContainerSpec, EnvironmentSpec, Image, ImageRef, ImageUse,
    IntoEnvironment, JobSpec, LayerSpec, PossiblyImage,
};
use serde::de::Error as _;
use serde::{de, Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::io::Read;

struct JobSpecIterator<InnerT> {
    inner: InnerT,
}

impl<InnerT> Iterator for JobSpecIterator<InnerT>
where
    InnerT: Iterator<Item = serde_json::Result<Job>>,
{
    type Item = Result<JobSpec>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.inner.next()? {
            Err(err) => Err(err.into()),
            Ok(job) => job.into_job_spec(),
        })
    }
}

pub fn job_spec_iter_from_reader(reader: impl Read) -> impl Iterator<Item = Result<JobSpec>> {
    let inner = serde_json::Deserializer::from_reader(reader).into_iter::<Job>();
    JobSpecIterator { inner }
}

#[derive(Debug, PartialEq)]
struct Job {
    program: Utf8PathBuf,
    arguments: Option<Vec<String>>,
    environment: Option<Vec<EnvironmentSpec>>,
    use_image_environment: bool,
    layers: PossiblyImage<NonEmpty<LayerSpec>>,
    added_layers: Vec<LayerSpec>,
    mounts: Option<Vec<JobMountForTomlAndJson>>,
    network: Option<JobNetwork>,
    enable_writable_file_system: Option<bool>,
    working_directory: Option<PossiblyImage<Utf8PathBuf>>,
    user: Option<UserId>,
    group: Option<GroupId>,
    image: Option<String>,
    timeout: Option<u32>,
    priority: Option<i8>,
}

impl Job {
    #[cfg(test)]
    fn new(program: Utf8PathBuf, layers: NonEmpty<LayerSpec>) -> Self {
        Job {
            program,
            layers: PossiblyImage::Explicit(layers),
            added_layers: Default::default(),
            arguments: None,
            environment: None,
            use_image_environment: false,
            mounts: None,
            network: None,
            enable_writable_file_system: None,
            working_directory: None,
            user: None,
            group: None,
            image: None,
            timeout: None,
            priority: None,
        }
    }

    fn into_job_spec(self) -> Result<JobSpec> {
        let environment = self.environment.unwrap_or_default();
        let mut image_use = EnumSet::new();
        if self.use_image_environment {
            if self.image.is_none() {
                bail!("no image provided");
            }
            image_use.insert(ImageUse::Environment);
        }
        let mut layers = match self.layers {
            PossiblyImage::Explicit(layers) => layers.into(),
            PossiblyImage::Image => {
                if self.image.is_none() {
                    bail!("no image provided");
                }
                image_use.insert(ImageUse::Layers);
                vec![]
            }
        };
        layers.extend(self.added_layers);
        let working_directory = match self.working_directory {
            None => None,
            Some(PossiblyImage::Explicit(working_directory)) => Some(working_directory),
            Some(PossiblyImage::Image) => {
                if self.image.is_none() {
                    bail!("no image provided");
                }
                image_use.insert(ImageUse::WorkingDirectory);
                None
            }
        };
        let parent = self.image.map(|image| {
            ContainerParent::Image(ImageRef {
                name: image,
                r#use: image_use,
            })
        });
        let container = ContainerSpec {
            parent,
            environment,
            layers,
            mounts: self
                .mounts
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            network: self.network.unwrap_or_default(),
            root_overlay: if self.enable_writable_file_system.unwrap_or_default() {
                JobRootOverlay::Tmp
            } else {
                JobRootOverlay::None
            },
            working_directory,
            user: self.user,
            group: self.group,
        };
        Ok(JobSpec {
            container,
            program: self.program,
            arguments: self.arguments.unwrap_or_default(),
            timeout: self.timeout.and_then(Timeout::new),
            estimated_duration: None,
            allocate_tty: None,
            priority: self.priority.unwrap_or_default(),
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
    AddedLayers,
    Mounts,
    Network,
    EnableWritableFileSystem,
    WorkingDirectory,
    User,
    Group,
    Image,
    Timeout,
    Priority,
}

struct JobVisitor;

fn must_be_image<T, E>(
    var: &Option<PossiblyImage<T>>,
    if_none: &str,
    if_explicit: &str,
) -> std::result::Result<(), E>
where
    E: de::Error,
{
    match var {
        None => Err(E::custom(format_args!("{}", if_none))),
        Some(PossiblyImage::Explicit(_)) => Err(E::custom(format_args!("{}", if_explicit))),
        Some(PossiblyImage::Image) => Ok(()),
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EnvSelector {
    Implicit(BTreeMap<String, String>),
    Explicit(Vec<EnvironmentSpec>),
}

impl IntoEnvironment for EnvSelector {
    fn into_environment(self) -> Vec<EnvironmentSpec> {
        match self {
            Self::Implicit(v) => v.into_environment(),
            Self::Explicit(v) => v,
        }
    }
}

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
        let mut environment: Option<EnvSelector> = None;
        let mut use_image_environment = false;
        let mut layers = None;
        let mut added_layers = None;
        let mut mounts = None;
        let mut network = None;
        let mut enable_writable_file_system = None;
        let mut working_directory = None;
        let mut user = None;
        let mut group = None;
        let mut image = None;
        let mut timeout = None;
        let mut priority = None;
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
                    if matches!(environment, Some(EnvSelector::Implicit(_)))
                        && use_image_environment
                    {
                        return Err(A::Error::custom(concat!(
                            "field `environment` must provide `extend` flags if `image` with a ",
                            "`use` of `environment` is also set"
                        )));
                    }
                }
                JobField::Layers => {
                    incompatible(
                        &layers,
                        concat!(
                            "field `layers` cannot be set if `image` with a `use` of ",
                            "`layers` is also set (try `added_layers` instead)"
                        ),
                    )?;
                    layers = Some(PossiblyImage::Explicit(
                        NonEmpty::from_vec(map.next_value()?).ok_or_else(|| {
                            de::Error::custom(format_args!("field `layers` cannot be empty"))
                        })?,
                    ));
                }
                JobField::AddedLayers => {
                    must_be_image(
                        &layers,
                        "field `added_layers` set before `image` with a `use` of `layers`",
                        "field `added_layers` cannot be set with `layer` field",
                    )?;
                    added_layers = Some(map.next_value()?);
                }
                JobField::Mounts => {
                    mounts = Some(map.next_value()?);
                }
                JobField::Network => {
                    network = Some(map.next_value()?);
                }
                JobField::EnableWritableFileSystem => {
                    enable_writable_file_system = Some(map.next_value()?);
                }
                JobField::WorkingDirectory => {
                    incompatible(
                        &working_directory,
                        concat!(
                            "field `working_directory` cannot be set if `image` with a `use` of ",
                            "`working_directory` is also set"
                        ),
                    )?;
                    working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                JobField::User => {
                    user = Some(map.next_value()?);
                }
                JobField::Group => {
                    group = Some(map.next_value()?);
                }
                JobField::Timeout => {
                    timeout = Some(map.next_value()?);
                }
                JobField::Priority => {
                    priority = Some(map.next_value()?);
                }
                JobField::Image => {
                    let i = map.next_value::<Image>()?;
                    image = Some(i.name);
                    for use_ in i.use_ {
                        match use_ {
                            ImageUse::WorkingDirectory => {
                                incompatible(
                                    &working_directory,
                                    concat!(
                                        "field `image` cannot use `working_directory` if field ",
                                        "`working_directory` is also set",
                                    ),
                                )?;
                                working_directory = Some(PossiblyImage::Image);
                            }
                            ImageUse::Layers => {
                                incompatible(
                                    &layers,
                                    concat!(
                                        "field `image` cannot use `layers` if field `layers` ",
                                        "is also set"
                                    ),
                                )?;
                                layers = Some(PossiblyImage::Image);
                            }
                            ImageUse::Environment => {
                                use_image_environment = true;
                                if matches!(environment, Some(EnvSelector::Implicit(_))) {
                                    return Err(A::Error::custom(concat!(
                                        "field `image` cannot use `environment` if `environment` ",
                                        "has not provided `extend` flags"
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Job {
            program: program.ok_or_else(|| de::Error::missing_field("program"))?,
            arguments,
            environment: environment.map(|e| e.into_environment()),
            use_image_environment,
            layers: layers.ok_or_else(|| de::Error::missing_field("layers"))?,
            added_layers: added_layers.unwrap_or_default(),
            mounts,
            network,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            image,
            timeout,
            priority,
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
mod tests {
    use super::*;
    use maelstrom_base::{enum_set, nonempty, JobDevice, JobDeviceForTomlAndJson, JobMount};
    use maelstrom_client::{image_container_parent, job_spec};
    use maelstrom_test::{non_root_utf8_path_buf, string_vec, tar_layer, utf8_path_buf};
    use maplit::btreemap;

    #[test]
    fn minimum_into_job_spec() {
        assert_eq!(
            Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
                .into_job_spec()
                .unwrap(),
            job_spec!("program", layers: [tar_layer!("1")]),
        );
    }

    #[test]
    fn most_into_job_spec() {
        assert_eq!(
            Job {
                arguments: Some(string_vec!["arg1", "arg2"]),
                environment: Some([("FOO", "foo"), ("BAR", "bar")].into_environment()),
                mounts: Some(vec![
                    JobMountForTomlAndJson::Tmp {
                        mount_point: non_root_utf8_path_buf!("/tmp"),
                    },
                    JobMountForTomlAndJson::Devices {
                        devices: enum_set! {JobDeviceForTomlAndJson::Null},
                    },
                ]),
                working_directory: Some(PossiblyImage::Explicit("/working-directory".into())),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "program",
                layers: [tar_layer!("1")],
                arguments: ["arg1", "arg2"],
                environment: [("BAR", "bar"), ("FOO", "foo")],
                mounts: [
                    JobMount::Tmp {
                        mount_point: utf8_path_buf!("/tmp"),
                    },
                    JobMount::Devices {
                        devices: enum_set! {JobDevice::Null},
                    },
                ],
                working_directory: "/working-directory",
                user: 101,
                group: 202,
            },
        );
    }

    #[test]
    fn network_none_into_job_spec() {
        assert_eq!(
            Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
                .into_job_spec()
                .unwrap(),
            job_spec!("program", layers: [tar_layer!("1")], network: JobNetwork::Disabled),
        );
    }

    #[test]
    fn network_disabled_into_job_spec() {
        assert_eq!(
            Job {
                network: Some(JobNetwork::Disabled),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec()
            .unwrap(),
            job_spec!("program", layers: [tar_layer!("1")], network: JobNetwork::Disabled),
        );
    }

    #[test]
    fn network_loopback_into_job_spec() {
        assert_eq!(
            Job {
                network: Some(JobNetwork::Loopback),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec()
            .unwrap(),
            job_spec!("program", layers: [tar_layer!("1")], network: JobNetwork::Loopback),
        );
    }

    #[test]
    fn network_local_into_job_spec() {
        assert_eq!(
            Job {
                network: Some(JobNetwork::Local),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec()
            .unwrap(),
            job_spec!("program", layers: [tar_layer!("1")], network: JobNetwork::Local),
        );
    }

    #[test]
    fn enable_writable_file_system_into_job_spec() {
        assert_eq!(
            Job {
                enable_writable_file_system: Some(true),
                ..Job::new(utf8_path_buf!("program"), nonempty![tar_layer!("1")])
            }
            .into_job_spec()
            .unwrap(),
            job_spec!("program", layers: [tar_layer!("1")], root_overlay: JobRootOverlay::Tmp),
        );
    }

    fn parse_job(str_: &str) -> serde_json::Result<Job> {
        serde_json::from_str(str_)
    }

    fn assert_error(err: serde_json::Error, expected: &str) {
        let message = format!("{err}");
        assert!(
            message.starts_with(expected),
            "message: {message:?}, expected: {expected:?}"
        );
    }

    #[test]
    fn basic() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")]),
        );
    }

    #[test]
    fn missing_program() {
        assert_error(
            parse_job(
                r#"{
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "missing field `program`",
        );
    }

    #[test]
    fn missing_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh"
                }"#,
            )
            .unwrap_err(),
            "missing field `layers`",
        );
    }

    #[test]
    fn empty_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": []
                }"#,
            )
            .unwrap_err(),
            "field `layers` cannot be empty",
        );
    }

    #[test]
    fn layers_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    }
                }"#
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                parent: image_container_parent!("image1", layers),
            },
        );
    }

    #[test]
    fn layers_after_layers_from_image() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    },
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "field `layers` cannot be set if `image` with a `use` of `layers` is also set",
        );
    }

    #[test]
    fn layers_from_image_after_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    }
                }"#,
            )
            .unwrap_err(),
            "field `image` cannot use `layers` if field `layers` is also set",
        );
    }

    #[test]
    fn added_layers_after_layers_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    },
                    "added_layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", layers),
            },
        );
    }

    #[test]
    fn added_layers_only() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` set before `image` with a `use` of `layers`",
        );
    }

    #[test]
    fn added_layers_before_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ { "tar": "3" } ],
                    "layers": [ { "tar": "1" }, { "tar": "2" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` set before `image` with a `use` of `layers`",
        );
    }

    #[test]
    fn added_layers_after_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" }, { "tar": "2" } ],
                    "added_layers": [ { "tar": "3" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` cannot be set with `layer` field",
        );
    }

    #[test]
    fn added_layers_before_image_with_layers() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ "3" ],
                    "image": { "name": "image1", "use": [ "layers" ] }
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` set before `image` with a `use` of `layers`",
        );
    }

    #[test]
    fn arguments() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "arguments": [ "-e", "echo foo" ]
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                arguments: ["-e", "echo foo"],
            },
        )
    }

    #[test]
    fn environment() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" }
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                environment: [("BAR", "bar"), ("FOO", "foo")],
            },
        )
    }

    #[test]
    fn environment_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", environment),
            },
        )
    }

    #[test]
    fn environment_from_image_after_implicit_environment() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" },
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap_err(),
            "field `image` cannot use `environment` if `environment` has not provided `extend` flags"
        )
    }

    #[test]
    fn environment_from_image_after_explicit_environment() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": [ { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true } ],
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", environment),
                environment: [
                    EnvironmentSpec {
                        vars: btreemap! {
                            "FOO".into() => "foo".into(),
                            "BAR".into() => "bar".into(),
                        },
                        extend: true,
                    }
                ],
            },
        )
    }

    #[test]
    fn implicit_environment_after_environment_from_image() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] },
                    "environment": { "FOO": "foo", "BAR": "bar" }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `environment` must provide `extend` flags if `image` with a `use` of ",
                "`environment` is also set"
            ),
        )
    }

    #[test]
    fn explicit_environment_after_environment_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] },
                    "environment": [ { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true } ]
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", environment),
                environment: [
                    EnvironmentSpec {
                        vars: btreemap! {
                            "FOO".into() => "foo".into(),
                            "BAR".into() => "bar".into(),
                        },
                        extend: true,
                    }
                ],
            },
        )
    }

    #[test]
    fn multi_explicit_environment() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] },
                    "environment": [
                        { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true },
                        { "vars": { "BAZ": "baz", "QUX": "qux" }, "extend": false }
                    ]
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", environment),
                environment: [
                    EnvironmentSpec {
                        vars: btreemap! {
                            "FOO".into() => "foo".into(),
                            "BAR".into() => "bar".into(),
                        },
                        extend: true,
                    },
                    EnvironmentSpec {
                        vars: btreemap! {
                            "BAZ".into() => "baz".into(),
                            "QUX".into() => "qux".into(),
                        },
                        extend: false,
                    },
                ],
            },
        )
    }

    #[test]
    fn devices() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "mounts": [
                        { "type": "devices", "devices": [ "null", "zero" ] }
                    ]
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                mounts: [
                    JobMount::Devices {
                        devices: enum_set! { JobDevice::Null | JobDevice::Zero },
                    },
                ],
            },
        )
    }

    #[test]
    fn mounts() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "mounts": [
                        { "type": "tmp", "mount_point": "/tmp" },
                        { "type": "bind", "mount_point": "/bind", "local_path": "/a" },
                        { "type": "bind", "mount_point": "/bind2", "local_path": "/b", "read_only": false },
                        { "type": "bind", "mount_point": "/bind3", "local_path": "/c", "read_only": true }
                    ]
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                mounts: [
                    JobMount::Tmp { mount_point: utf8_path_buf!("/tmp") },
                    JobMount::Bind {
                        mount_point: utf8_path_buf!("/bind"),
                        local_path: utf8_path_buf!("/a"),
                        read_only: false,
                    },
                    JobMount::Bind {
                        mount_point: utf8_path_buf!("/bind2"),
                        local_path: utf8_path_buf!("/b"),
                        read_only: false,
                    },
                    JobMount::Bind {
                        mount_point: utf8_path_buf!("/bind3"),
                        local_path: utf8_path_buf!("/c"),
                        read_only: true,
                    },
                ],
            },
        )
    }

    #[test]
    fn foo() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "network": "loopback"
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], network: JobNetwork::Loopback),
        )
    }

    #[test]
    fn enable_writable_file_system() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "enable_writable_file_system": true
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], root_overlay: JobRootOverlay::Tmp),
        )
    }

    #[test]
    fn working_directory() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "working_directory": "/foo/bar"
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], working_directory: "/foo/bar"),
        )
    }

    #[test]
    fn working_directory_from_image() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    }
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", working_directory),
            },
        )
    }

    #[test]
    fn working_directory_from_image_after_working_directory() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "working_directory": "/foo/bar",
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    }
                }"#,
            )
            .unwrap_err(),
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        )
    }

    #[test]
    fn working_directory_after_working_directory_from_image() {
        assert_error(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    },
                    "working_directory": "/foo/bar"
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `working_directory` cannot be set if `image` with a `use` of ",
                "`working_directory` is also set",
            ),
        )
    }

    #[test]
    fn user() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "user": 1234
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], user: 1234),
        )
    }

    #[test]
    fn group() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "group": 4321
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], group: 4321),
        )
    }

    #[test]
    fn timeout() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "timeout": 1234
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], timeout: 1234),
        )
    }

    #[test]
    fn timeout_0() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "timeout": 0
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], timeout: 0),
        )
    }

    #[test]
    fn priority() {
        assert_eq!(
            parse_job(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "priority": -42
                }"#,
            )
            .unwrap()
            .into_job_spec()
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], priority: -42),
        )
    }
}
