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
    use maelstrom_client::container_spec;
    use maelstrom_test::tar_layer;
    use maplit::hashmap;

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
    fn basic_container_map() {
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
                }"#})
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
            parse_container_map(indoc! {r#"{
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
            }"#})
            .unwrap_err(),
            concat!(
                "field `layers` cannot be set if `image` with an explicit `use` ",
                "of `layers` is also specified (try `added_layers` instead)",
            ),
        );
    }
}
