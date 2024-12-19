//! An iterator of objects read from stdin or the command-line-specified file.

use anyhow::Result;
use maelstrom_client::spec::{ContainerSpec, JobSpec};
use serde::{
    de::Deserializer,
    Deserialize,
    __private::de::{Content, ContentRefDeserializer},
};
use std::{collections::HashMap, io::Read};

/// These are the objects that we read from stdin or the file specified on the command-line. We
/// expect a series of these. We will execute each job immediately after we parse it.
///
/// We want to allow the specification of named containers so that `maelstrom-run` has the same
/// capabilities as `cargo-maelstrom`, etc. To do so, we allow a special pseudo-job which just
/// specifies a map of containers. An input stream may have any number of these.
///
/// To identify when an object should be parsed as a `JobSpec` and when to parse it as a map of
/// containers, we look for an object with the single key `containers`. For example:
/// ```json
///     { "program": "my-program", "image": "my-image" }
///     {
///         "containers": {
///             "my-grandparent": {
///                 "image": "my-image",
///                 "mounts": { ... }
///             },
///             "my-parent": {
///                 "parent": "my-grandparent",
///                 "user": 101
///             }
///         }
///     }
///     { "program": "my-program", "parent": "my-parent" }
/// ```
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum JobSpecOrContainers {
    JobSpec(JobSpec),
    Containers(HashMap<String, ContainerSpec>),
}

impl JobSpecOrContainers {
    pub fn iter_from_json_reader(
        reader: impl Read,
    ) -> impl Iterator<Item = serde_json::Result<Self>> {
        serde_json::Deserializer::from_reader(reader).into_iter::<Self>()
    }
}

impl<'de> Deserialize<'de> for JobSpecOrContainers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let content = Content::deserialize(deserializer)?;
        if let Content::Map(fields) = &content {
            if let [(key, value)] = fields.as_slice() {
                if let Some(key) = key.as_str() {
                    if key == "containers" {
                        return HashMap::<String, ContainerSpec>::deserialize(
                            ContentRefDeserializer::<D::Error>::new(value),
                        )
                        .map(Self::Containers);
                    }
                }
            }
        }
        JobSpec::deserialize(ContentRefDeserializer::<D::Error>::new(&content)).map(Self::JobSpec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use maelstrom_base::JobNetwork;
    use maelstrom_client::{
        container_container_parent, container_spec, environment_spec, job_spec,
    };
    use maelstrom_test::tar_layer;
    use maplit::hashmap;

    #[track_caller]
    fn parse(file: &str) -> serde_json::Result<JobSpecOrContainers> {
        serde_json::from_str(file)
    }

    #[track_caller]
    fn parse_job_spec(file: &str) -> JobSpec {
        if let JobSpecOrContainers::JobSpec(job_spec) = parse(file).unwrap() {
            job_spec
        } else {
            panic!("expected JobSpec")
        }
    }

    #[track_caller]
    fn parse_container_map(file: &str) -> HashMap<String, ContainerSpec> {
        if let JobSpecOrContainers::Containers(containers) = parse(file).unwrap() {
            containers
        } else {
            panic!("expected HashMap<String, ContainerSpec>")
        }
    }

    #[track_caller]
    fn assert_parse_error(file: &str, expected: &str) {
        let message = format!("{}", parse(file).unwrap_err());
        assert!(
            message.starts_with(expected),
            "message: {message:?}, expected: {expected:?}"
        );
    }

    #[test]
    fn job_spec() {
        assert_eq!(
            parse_job_spec(indoc! {r#"{
                "program": "/bin/sh",
                "parent": "parent",
                "layers": [ { "tar": "1" }, { "tar": "2" } ],
                "added_environment": { "FOO": "foo" },
                "working_directory": "/root"
            }"#}),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1"), tar_layer!("2")],
                environment: environment_spec!(true, "FOO" => "foo"),
                working_directory: "/root",
                parent: container_container_parent!("parent", all, -layers, -working_directory),
            },
        );
    }

    #[test]
    fn container_map() {
        assert_eq!(
            parse_container_map(indoc! {r#"{
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
            }"#}),
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
    fn error_in_container() {
        assert_parse_error(
            indoc! {r#"{
                "containers": {
                    "container-1": {
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }
                }
            }"#},
            concat!(
                "field `layers` cannot be set if `image` with an explicit `use` ",
                "of `layers` is also specified (try `added_layers` instead)",
            ),
        );
    }

    #[test]
    fn containers_and_job_spec_field_yields_job_spec_error() {
        assert_parse_error(
            indoc! {r#"{
                "containers": {
                    "container-1": {
                        "layers": [ { "tar": "1" } ],
                        "user": 101
                    }
                },
                "program": "/bin/sh"
            }"#},
            "unknown field `containers`",
        );
    }

    #[test]
    fn containers_field_wrong_type_yields_type_error() {
        assert_parse_error(
            indoc! {r#"{
                "containers": 42
            }"#},
            "invalid type: integer `42`, expected a map",
        );
    }
}
