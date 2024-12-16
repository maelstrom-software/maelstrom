use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf};
use maelstrom_client::spec::{
    ContainerParent, ContainerRef, ContainerSpec, ContainerUse, EnvironmentSpec, ImageRef,
    ImageUse, IntoEnvironment, JobSpec, LayerSpec,
};
use serde::{
    de::Deserializer,
    Deserialize,
    __private::de::{Content, ContentRefDeserializer},
};
use std::{
    collections::{BTreeMap, HashMap},
    io::Read,
};

pub fn job_spec_or_containers_iter_from_reader(
    reader: impl Read,
) -> impl Iterator<Item = serde_json::Result<JobSpecOrContainers>> {
    serde_json::Deserializer::from_reader(reader).into_iter::<JobSpecOrContainers>()
}

#[derive(Deserialize)]
#[serde(try_from = "JobSpecOrContainersForDeserialize")]
#[allow(clippy::large_enum_variant)]
pub enum JobSpecOrContainers {
    JobSpec(JobSpec),
    Containers(HashMap<String, ContainerSpec>),
}

impl TryFrom<JobSpecOrContainersForDeserialize> for JobSpecOrContainers {
    type Error = String;

    fn try_from(job: JobSpecOrContainersForDeserialize) -> Result<Self, Self::Error> {
        Ok(match job {
            JobSpecOrContainersForDeserialize::JobSpec(job_spec) => {
                JobSpecOrContainers::JobSpec(job_spec.try_into()?)
            }
            JobSpecOrContainersForDeserialize::Containers(containers) => {
                JobSpecOrContainers::Containers(
                    containers
                        .containers
                        .into_iter()
                        .map(|(name, container)| (name, container.0))
                        .collect(),
                )
            }
        })
    }
}

enum JobSpecOrContainersForDeserialize {
    JobSpec(JobSpecForDeserialize),
    Containers(ContainerMapForDeserialize),
}

impl<'de> Deserialize<'de> for JobSpecOrContainersForDeserialize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let content = Content::deserialize(deserializer)?;
        if let Content::Map(fields) = &content {
            if let [(key, _)] = fields.as_slice() {
                if let Some(key) = key.as_str() {
                    if key == "containers" {
                        return ContainerMapForDeserialize::deserialize(ContentRefDeserializer::<
                            D::Error,
                        >::new(
                            &content
                        ))
                        .map(Self::Containers);
                    }
                }
            }
        }
        JobSpecForDeserialize::deserialize(ContentRefDeserializer::<D::Error>::new(&content))
            .map(Self::JobSpec)
    }
}

#[derive(Deserialize)]
struct JobSpecForDeserialize {
    #[serde(flatten)]
    container: ContainerSpecForDeserialize,
    program: Utf8PathBuf,
    arguments: Option<Vec<String>>,
    timeout: Option<u32>,
    priority: Option<i8>,
}

impl TryFrom<JobSpecForDeserialize> for JobSpec {
    type Error = String;

    fn try_from(job_spec: JobSpecForDeserialize) -> Result<Self, Self::Error> {
        let JobSpecForDeserialize {
            container,
            program,
            arguments,
            timeout,
            priority,
        } = job_spec;
        Ok(JobSpec {
            container: ContainerSpec::try_from(container)?,
            program,
            arguments: arguments.unwrap_or_default(),
            timeout: timeout.and_then(Timeout::new),
            estimated_duration: None,
            allocate_tty: None,
            priority: priority.unwrap_or_default(),
            capture_file_system_changes: None,
        })
    }
}

#[derive(Deserialize)]
struct ContainerSpecForDeserialize {
    environment: Option<EnvSelector>,
    layers: Option<Vec<LayerSpec>>,
    added_layers: Option<Vec<LayerSpec>>,
    mounts: Option<Vec<JobMountForTomlAndJson>>,
    network: Option<JobNetwork>,
    enable_writable_file_system: Option<bool>,
    working_directory: Option<Utf8PathBuf>,
    user: Option<UserId>,
    group: Option<GroupId>,
    image: Option<ImageRef>,
    parent: Option<ContainerRef>,
}

impl TryFrom<ContainerSpecForDeserialize> for ContainerSpec {
    type Error = String;

    fn try_from(container: ContainerSpecForDeserialize) -> Result<Self, Self::Error> {
        let ContainerSpecForDeserialize {
            environment,
            layers,
            added_layers,
            mounts,
            network,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            image,
            parent,
        } = container;

        let image_use = image
            .as_ref()
            .map(|image_ref| image_ref.r#use)
            .unwrap_or_default();
        let parent_use = parent
            .as_ref()
            .map(|container_ref| container_ref.r#use)
            .unwrap_or_default();

        if image.is_some() && parent.is_some() {
            return Err("both `image` and `parent` cannot be specified".into());
        }

        if added_layers.is_some() {
            if layers.is_some() {
                return Err("field `added_layers` cannot be set with `layers` field".into());
            } else if image.is_some() && !image_use.contains(ImageUse::Layers) {
                return Err(concat!(
                    "field `added_layers` cannot be set without ",
                    "`image` with a `use` of `layers` also being specified",
                )
                .into());
            } else if parent.is_some() && !parent_use.contains(ContainerUse::Layers) {
                return Err(concat!(
                    "field `added_layers` cannot be set without ",
                    "`parent` with a `use` of `layers` also being specified",
                )
                .into());
            } else if image.is_none() && parent.is_none() {
                return Err(concat!(
                    "field `added_layers` cannot be set without ",
                    "`image` or `parent` with a `use` of `layers` also being specified",
                )
                .into());
            }
        }

        if layers.is_some() {
            if image_use.contains(ImageUse::Layers) {
                return Err(concat!(
                    "field `layers` cannot be set if `image` with a `use` of ",
                    "`layers` is also specified (try `added_layers` instead)",
                )
                .into());
            } else if parent_use.contains(ContainerUse::Layers) {
                return Err(concat!(
                    "field `layers` cannot be set if `parent` with a `use` of ",
                    "`layers` is also specified (try `added_layers` instead)",
                )
                .into());
            }
        }
        if layers.is_none()
            && !image_use.contains(ImageUse::Layers)
            && !parent_use.contains(ContainerUse::Layers)
        {
            if image.is_some() {
                return Err(
                    "either field `layers` must be set or `image` must specify a `use` of `layers`"
                        .into(),
                );
            } else if parent.is_some() {
                return Err(
                    "either field `layers` must be set or `parent` must specify a `use` of `layers`"
                        .into(),
                );
            } else {
                return Err(concat!(
                    "either field `layers` must be set or an `image` or `parent` with a `use` of ",
                    "`layers` must be specified",
                )
                .into());
            }
        }
        if let Some([]) = layers.as_deref() {
            return Err("field `layers` cannot be empty".into());
        }

        if matches!(environment, Some(EnvSelector::Implicit(_))) {
            if image_use.contains(ImageUse::Environment) {
                return Err(concat!(
                    "field `environment` must provide `extend` flags if `image` with a ",
                    "`use` of `environment` is also specified",
                )
                .into());
            } else if parent_use.contains(ContainerUse::Environment) {
                return Err(concat!(
                    "field `environment` must provide `extend` flags if `parent` with a ",
                    "`use` of `environment` is also specified",
                )
                .into());
            }
        }

        if working_directory.is_some() {
            if image_use.contains(ImageUse::WorkingDirectory) {
                return Err(concat!(
                    "field `working_directory` cannot be set if `image` with a `use` of ",
                    "`working_directory` is also specified",
                )
                .into());
            } else if parent_use.contains(ContainerUse::WorkingDirectory) {
                return Err(concat!(
                    "field `working_directory` cannot be set if `parent` with a `use` of ",
                    "`working_directory` is also specified",
                )
                .into());
            }
        }

        Ok(ContainerSpec {
            parent: match (image, parent) {
                (Some(image), _) => Some(ContainerParent::Image(image)),
                (_, Some(parent)) => Some(ContainerParent::Container(parent)),
                (None, None) => None,
            },
            layers: layers.into_iter().chain(added_layers).flatten().collect(),
            enable_writable_file_system,
            environment: environment
                .map(IntoEnvironment::into_environment)
                .unwrap_or_default(),
            working_directory,
            mounts: mounts
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            network,
            user,
            group,
        })
    }
}

#[derive(Deserialize)]
struct ContainerMapForDeserialize {
    containers: HashMap<String, ContainerMapForDeserializeElement>,
}

#[derive(Deserialize)]
#[serde(try_from = "ContainerSpecForDeserialize")]
struct ContainerMapForDeserializeElement(ContainerSpec);

impl TryFrom<ContainerSpecForDeserialize> for ContainerMapForDeserializeElement {
    type Error = <ContainerSpec as TryFrom<ContainerSpecForDeserialize>>::Error;

    fn try_from(container: ContainerSpecForDeserialize) -> Result<Self, Self::Error> {
        Ok(Self(ContainerSpec::try_from(container)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::{enum_set, JobDevice, JobMount};
    use maelstrom_client::{
        container_container_parent, container_spec, image_container_parent, job_spec,
    };
    use maelstrom_test::{tar_layer, utf8_path_buf};
    use maplit::{btreemap, hashmap};

    fn parse_job_spec(str_: &str) -> serde_json::Result<JobSpec> {
        serde_json::from_str(str_).map(|job_spec: JobSpecOrContainers| {
            let JobSpecOrContainers::JobSpec(job_spec) = job_spec else {
                panic!("expected JobSpec")
            };
            job_spec
        })
    }

    fn parse_container_map(str_: &str) -> serde_json::Result<HashMap<String, ContainerSpec>> {
        serde_json::from_str(str_).map(|containers: JobSpecOrContainers| {
            let JobSpecOrContainers::Containers(containers) = containers else {
                panic!("expected HashMap<String, ContainerSpec>")
            };
            containers
        })
    }

    #[track_caller]
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
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")]),
        );
    }

    #[test]
    fn missing_program() {
        assert_error(
            parse_job_spec(
                r#"{
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            "missing field `program`",
        );
    }

    #[test]
    fn image_and_parent() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "image": "image",
                    "parent": "parent"
                }"#,
            )
            .unwrap_err(),
            "both `image` and `parent` cannot be specified",
        );
    }

    #[test]
    fn added_layers_and_layers() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" }, { "tar": "2" } ],
                    "added_layers": [ { "tar": "3" } ]
                }"#,
            )
            .unwrap_err(),
            "field `added_layers` cannot be set with `layers` field",
        );
    }

    #[test]
    fn added_layers_and_image_with_layers() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    },
                    "added_layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", layers),
            },
        );
    }

    #[test]
    fn added_layers_and_image_without_layers() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "environment" ]
                    },
                    "added_layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `added_layers` cannot be set without `image` with a ",
                "`use` of `layers` also being specified",
            ),
        );
    }

    #[test]
    fn added_layers_and_parent_with_layers() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "parent": {
                        "name": "parent",
                        "use": [ "layers" ]
                    },
                    "added_layers": [ { "tar": "1" } ]
                }"#
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: container_container_parent!("parent", layers),
            },
        );
    }

    #[test]
    fn added_layers_and_parent_without_layers() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "parent": {
                        "name": "parent",
                        "use": [ "environment" ]
                    },
                    "added_layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `added_layers` cannot be set without `parent` with a ",
                "`use` of `layers` also being specified",
            ),
        );
    }

    #[test]
    fn added_layers_and_no_image_nor_parent() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "added_layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `added_layers` cannot be set without `image` or `parent` ",
                "with a `use` of `layers` also being specified",
            ),
        );
    }

    #[test]
    fn image_with_layers() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "layers" ]
                    }
                }"#
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                parent: image_container_parent!("image1", layers),
            },
        );
    }

    #[test]
    fn parent_with_layers() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "parent": {
                        "name": "parent",
                        "use": [ "layers" ]
                    }
                }"#
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                parent: container_container_parent!("parent", layers),
            },
        );
    }

    #[test]
    fn layers_and_image_with_layers() {
        assert_error(
            parse_job_spec(
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
            concat!(
                "field `layers` cannot be set if `image` with a `use` of `layers` ",
                "is also specified (try `added_layers` instead)",
            ),
        );
    }

    #[test]
    fn layers_and_image_without_layers() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "environment" ]
                    },
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", environment),
            },
        );
    }

    #[test]
    fn layers_and_parent_with_layers() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "parent": {
                        "name": "parent",
                        "use": [ "layers" ]
                    },
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `layers` cannot be set if `parent` with a `use` of `layers` ",
                "is also specified (try `added_layers` instead)",
            ),
        );
    }

    #[test]
    fn layers_and_parent_without_layers() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "parent": {
                        "name": "parent",
                        "use": [ "environment" ]
                    },
                    "layers": [ { "tar": "1" } ]
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: container_container_parent!("parent", environment),
            },
        );
    }

    #[test]
    fn no_layers_and_no_image_nor_parent() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh"
                }"#,
            )
            .unwrap_err(),
            concat!(
                "either field `layers` must be set or an `image` or `parent` ",
                "with a `use` of `layers` must be specified",
            ),
        );
    }

    #[test]
    fn no_layers_and_image_without_layers() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "image": {
                        "name": "image1",
                        "use": [ "environment" ]
                    }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "either field `layers` must be set or ",
                "`image` must specify a `use` of `layers`",
            ),
        );
    }

    #[test]
    fn no_layers_and_parent_without_layers() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "parent": {
                        "name": "parent",
                        "use": [ "environment" ]
                    }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "either field `layers` must be set or ",
                "`parent` must specify a `use` of `layers`",
            ),
        );
    }

    #[test]
    fn empty_layers() {
        assert_error(
            parse_job_spec(
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
    fn arguments() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "arguments": [ "-e", "echo foo" ]
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                arguments: ["-e", "echo foo"],
            },
        )
    }

    #[test]
    fn implicit_environment() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" }
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                environment: [("BAR", "bar"), ("FOO", "foo")],
            },
        )
    }

    #[test]
    fn explicit_environment() {
        assert_eq!(
            parse_job_spec(
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
    fn image_with_environment() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", environment),
            },
        )
    }

    #[test]
    fn parent_with_environment() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "parent": { "name": "parent", "use": [ "environment" ] }
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: container_container_parent!("parent", environment),
            },
        )
    }

    #[test]
    fn implicit_environment_and_image_with_environment() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" },
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `environment` must provide `extend` flags if `image` with a ",
                "`use` of `environment` is also specified",
            ),
        )
    }

    #[test]
    fn explicit_environment_and_image_with_environment() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": [ { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true } ],
                    "image": { "name": "image1", "use": [ "environment" ] }
                }"#,
            )
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
    fn implicit_environment_and_parent_with_environment() {
        assert_error(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": { "FOO": "foo", "BAR": "bar" },
                    "parent": { "name": "parent", "use": [ "environment" ] }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `environment` must provide `extend` flags if `parent` with a ",
                "`use` of `environment` is also specified",
            ),
        )
    }

    #[test]
    fn explicit_environment_and_parent_with_environment() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "environment": [ { "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true } ],
                    "parent": { "name": "parent", "use": [ "environment" ] }
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: container_container_parent!("parent", environment),
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
    fn devices() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "mounts": [
                        { "type": "devices", "devices": [ "null", "zero" ] }
                    ]
                }"#,
            )
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
            parse_job_spec(
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
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "network": "loopback"
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], network: JobNetwork::Loopback),
        )
    }

    #[test]
    fn enable_writable_file_system() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "enable_writable_file_system": true
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], enable_writable_file_system: true),
        )
    }

    #[test]
    fn working_directory() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "working_directory": "/foo/bar"
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], working_directory: "/foo/bar"),
        )
    }

    #[test]
    fn working_directory_from_image() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "image": {
                        "name": "image1",
                        "use": [ "working_directory" ]
                    }
                }"#,
            )
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                parent: image_container_parent!("image1", working_directory),
            },
        )
    }

    #[test]
    fn working_directory_from_image_and_working_directory() {
        assert_error(
            parse_job_spec(
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
            concat!(
                "field `working_directory` cannot be set if `image` with a `use` of ",
                "`working_directory` is also specified",
            ),
        )
    }

    #[test]
    fn working_directory_and_working_directory_from_image() {
        assert_error(
            parse_job_spec(
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
                "`working_directory` is also specified",
            ),
        )
    }

    #[test]
    fn user() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "user": 1234
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], user: 1234),
        )
    }

    #[test]
    fn group() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "group": 4321
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], group: 4321),
        )
    }

    #[test]
    fn timeout() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "timeout": 1234
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], timeout: 1234),
        )
    }

    #[test]
    fn timeout_0() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "timeout": 0
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], timeout: 0),
        )
    }

    #[test]
    fn priority() {
        assert_eq!(
            parse_job_spec(
                r#"{
                    "program": "/bin/sh",
                    "layers": [ { "tar": "1" } ],
                    "priority": -42
                }"#,
            )
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], priority: -42),
        )
    }

    #[test]
    fn basic_container_map() {
        assert_eq!(
            parse_container_map(
                r#"{
                    "containers": {
                        "container-1": {
                            "layers": [ { "tar": "1" } ],
                            "user": 101
                        },
                        "container-2": {
                            "layers": [ { "tar": "2" } ],
                            "network": "loopback"
                        }
                    }
                }"#
            )
            .unwrap(),
            hashmap! {
                "container-1".into() => container_spec!{
                    layers: [tar_layer!("1")],
                    user: 101,
                },
                "container-2".into() => container_spec!{
                    layers: [tar_layer!("2")],
                    network: JobNetwork::Loopback,
                },
            },
        );
    }

    #[test]
    fn basic_container_map_error() {
        assert_error(
            parse_container_map(
                r#"{
                    "containers": {
                        "container-1": {
                            "program": "/bin/sh",
                            "image": {
                                "name": "image1",
                                "use": [ "layers" ]
                            },
                            "layers": [ { "tar": "1" } ]
                        }
                    }
                }"#,
            )
            .unwrap_err(),
            concat!(
                "field `layers` cannot be set if `image` with a `use` of `layers` ",
                "is also specified (try `added_layers` instead)",
            ),
        );
    }
}
