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
    use maelstrom_base::{proc_mount, tmp_mount, JobMount, JobNetwork};
    use maelstrom_client::{
        container_container_parent, container_spec, environment_spec, image_container_parent,
        job_spec,
    };
    use maelstrom_test::{tar_layer, utf8_path_buf};
    use maplit::hashmap;

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
            parse_job_spec(indoc! {r#"{
                "program": "/bin/sh",
                "layers": [ { "tar": "1" } ]
            }"#})
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")]),
        );
    }

    #[test]
    fn missing_program() {
        assert_error(
            parse_job_spec(indoc! {r#"{
                "layers": [ { "tar": "1" } ]
            }"#})
            .unwrap_err(),
            "missing field `program`",
        );
    }

    #[test]
    fn arguments() {
        assert_eq!(
            parse_job_spec(indoc! {r#"{
                "program": "/bin/sh",
                "layers": [ { "tar": "1" } ],
                "arguments": [ "-e", "echo foo" ]
            }"#})
            .unwrap(),
            job_spec! {
                "/bin/sh",
                layers: [tar_layer!("1")],
                arguments: ["-e", "echo foo"],
            },
        )
    }

    #[test]
    fn timeout() {
        assert_eq!(
            parse_job_spec(indoc! {r#"{
                "program": "/bin/sh",
                "layers": [ { "tar": "1" } ],
                "timeout": 1234
            }"#})
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], timeout: 1234),
        )
    }

    #[test]
    fn timeout_0() {
        assert_eq!(
            parse_job_spec(indoc! {r#"{
                "program": "/bin/sh",
                "layers": [ { "tar": "1" } ],
                "timeout": 0
            }"#})
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], timeout: 0),
        )
    }

    #[test]
    fn priority() {
        assert_eq!(
            parse_job_spec(indoc! {r#"{
                "program": "/bin/sh",
                "layers": [ { "tar": "1" } ],
                "priority": -42
            }"#})
            .unwrap(),
            job_spec!("/bin/sh", layers: [tar_layer!("1")], priority: -42),
        )
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

    mod container_spec {
        use super::*;

        #[test]
        fn image_and_parent() {
            assert_error(
                parse_job_spec(indoc! {r#"{
                    "program": "/bin/sh",
                    "image": "image",
                    "parent": "parent"
                }"#})
                .unwrap_err(),
                "both `image` and `parent` cannot be specified",
            );
        }

        mod layers {
            use super::*;

            #[test]
            fn added_layers_and_layers() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" }, { "tar": "2" } ],
                        "added_layers": [ { "tar": "3" } ]
                    }"#})
                    .unwrap_err(),
                    "field `added_layers` cannot be set with `layers` field",
                );
            }

            #[test]
            fn added_layers_and_image_with_implicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1",
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn added_layers_and_image_with_explicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
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
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        },
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_layers` requires `image` being specified with a ",
                        "`use` of `layers` (try `layers` instead)",
                    ),
                );
            }

            #[test]
            fn added_layers_and_parent_with_implicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn added_layers_and_parent_with_explicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        },
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
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
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_layers` requires `parent` being specified with a ",
                        "`use` of `layers` (try `layers` instead)",
                    ),
                );
            }

            #[test]
            fn added_layers_and_neither_image_nor_parent() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "added_layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_layers` cannot be set without ",
                        "`image` or `parent` also being specified (try `layers` instead)",
                    ),
                );
            }

            #[test]
            fn empty_added_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "added_layers": []
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn image_with_implicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn image_with_explicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn parent_with_implicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_explicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn layers_and_image_with_implicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1",
                        "layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: image_container_parent!("image1", all, -layers),
                    },
                );
            }

            #[test]
            fn layers_and_image_with_explicit_layers() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `layers` cannot be set if `image` with an explicit `use` of ",
                        "`layers` is also specified (try `added_layers` instead)",
                    ),
                );
            }

            #[test]
            fn layers_and_image_without_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn layers_and_parent_with_implicit_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: container_container_parent!("parent", all, -layers),
                    },
                );
            }

            #[test]
            fn layers_and_parent_with_explicit_layers() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `layers` cannot be set if `parent` with an explicit `use` of ",
                        "`layers` is also specified (try `added_layers` instead)",
                    ),
                );
            }

            #[test]
            fn layers_and_parent_without_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "layers": [ { "tar": "1" } ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn no_layers_and_neither_image_nor_parent() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                    },
                );
            }

            #[test]
            fn no_layers_and_image_without_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn no_layers_and_parent_without_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn empty_layers() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": []
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                    },
                );
            }
        }

        mod environment {
            use super::*;

            #[test]
            fn added_environment_and_environment() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "environment": { "FOO": "foo", "BAR": "bar" },
                        "added_environment": { "FROB": "frob" }
                    }"#})
                    .unwrap_err(),
                    "field `added_environment` cannot be set with `environment` field",
                );
            }

            #[test]
            fn added_environment_and_image_with_implicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1",
                        "added_environment": { "FROB": "frob" }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: environment_spec!(true, "FROB" => "frob"),
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn added_environment_and_image_with_explicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        },
                        "added_environment": { "FROB": "frob" }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: environment_spec!(true, "FROB" => "frob"),
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn added_environment_and_image_without_environment() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "added_environment": { "FROB": "frob" }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_environment` requires `image` being specified with a ",
                        "`use` of `environment` (try `environment` instead)",
                    ),
                );
            }

            #[test]
            fn added_environment_and_parent_with_implicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "added_environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: [
                            environment_spec!(false, "FROB" => "frob"),
                            environment_spec!(true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn added_environment_and_parent_with_explicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "added_environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: [
                            environment_spec!(false, "FROB" => "frob"),
                            environment_spec!(true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn added_environment_and_parent_without_environment() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        },
                        "added_environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_environment` requires `parent` being specified with a ",
                        "`use` of `environment` (try `environment` instead)",
                    ),
                );
            }

            #[test]
            fn added_environment_and_neither_image_nor_parent() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "added_environment": { "FROB": "frob" }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_environment` cannot be set without ",
                        "`image` or `parent` also being specified (try `environment` instead)",
                    ),
                );
            }

            #[test]
            fn empty_added_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "added_environment": []
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn image_with_implicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", all),
                    },
                );
            }

            #[test]
            fn image_with_explicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", environment),
                    },
                );
            }

            #[test]
            fn parent_with_implicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_explicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn environment_and_image_with_implicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1",
                        "environment": { "FROB": "frob" }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: environment_spec!(true, "FROB" => "frob"),
                        parent: image_container_parent!("image1", all, -environment),
                    },
                );
            }

            #[test]
            fn environment_and_image_with_explicit_environment() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        },
                        "environment": { "FROB": "frob" }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `environment` cannot be set if `image` with an explicit `use` of ",
                        "`environment` is also specified (try `added_environment` instead)",
                    ),
                );
            }

            #[test]
            fn environment_and_image_without_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        },
                        "environment": { "FROB": "frob" }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: environment_spec!(true, "FROB" => "frob"),
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn environment_and_parent_with_implicit_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: [
                            environment_spec!(false, "FROB" => "frob"),
                            environment_spec!(true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", all, -environment),
                    },
                );
            }

            #[test]
            fn environment_and_parent_with_explicit_environment() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `environment` cannot be set if `parent` with an explicit `use` of ",
                        "`environment` is also specified (try `added_environment` instead)",
                    ),
                );
            }

            #[test]
            fn environment_and_parent_without_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        },
                        "environment": [
                            {
                                "vars": { "FROB": "frob" },
                                "extend": false
                            },
                            {
                                "vars": { "BAZ": "baz" },
                                "extend": true
                            }
                        ]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        environment: [
                            environment_spec!(false, "FROB" => "frob"),
                            environment_spec!(true, "BAZ" => "baz"),
                        ],
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn no_environment_and_neither_image_nor_parent() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                    },
                );
            }

            #[test]
            fn no_environment_and_image_without_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": {
                            "name": "image1",
                            "use": [ "layers" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", layers),
                    },
                );
            }

            #[test]
            fn no_environment_and_parent_without_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "layers" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", layers),
                    },
                );
            }

            #[test]
            fn empty_environment() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "environment": []
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                    },
                );
            }
        }

        mod working_directory {
            use super::*;

            #[test]
            fn working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar"
                    }"#})
                    .unwrap(),
                    job_spec!("/bin/sh", layers: [tar_layer!("1")], working_directory: "/foo/bar"),
                )
            }

            #[test]
            fn image_with_implicit_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "image": "image1"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: image_container_parent!("image1", all),
                    },
                )
            }

            #[test]
            fn image_with_explicit_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "image": {
                            "name": "image1",
                            "use": [ "working_directory" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: image_container_parent!("image1", working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_image_with_implicit_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "working_directory": "/foo/bar",
                        "image": "image1"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        working_directory: "/foo/bar",
                        parent: image_container_parent!("image1", all, -working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_image_with_explicit_working_directory() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar",
                        "image": {
                            "name": "image1",
                            "use": [ "working_directory" ]
                        }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `working_directory` cannot be set if `image` with an explicit `use` of ",
                        "`working_directory` is also specified",
                    ),
                )
            }

            #[test]
            fn working_directory_and_image_without_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar",
                        "image": {
                            "name": "image1",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        working_directory: "/foo/bar",
                        parent: image_container_parent!("image1", environment),
                    },
                )
            }

            #[test]
            fn parent_with_implicit_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "parent": {
                            "name": "parent",
                            "use": [ "working_directory" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        parent: container_container_parent!("parent", working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_parent_with_implicit_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "working_directory": "/foo/bar",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        working_directory: "/foo/bar",
                        parent: container_container_parent!("parent", all, -working_directory),
                    },
                )
            }

            #[test]
            fn working_directory_and_parent_with_explicit_working_directory() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar",
                        "parent": {
                            "name": "parent",
                            "use": [ "working_directory" ]
                        }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `working_directory` cannot be set if `parent` with an explicit `use` of ",
                        "`working_directory` is also specified",
                    ),
                )
            }

            #[test]
            fn working_directory_and_parent_without_working_directory() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "working_directory": "/foo/bar",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        layers: [tar_layer!("1")],
                        working_directory: "/foo/bar",
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod enable_writable_file_system {
            use super::*;

            #[test]
            fn enable_writable_file_system() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "enable_writable_file_system": true
                    }"#})
                    .unwrap(),
                    job_spec!("/bin/sh", enable_writable_file_system: true),
                )
            }

            #[test]
            fn parent_with_implicit_enable_writable_file_system() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_enable_writable_file_system() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "enable_writable_file_system" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", enable_writable_file_system),
                    },
                )
            }

            #[test]
            fn enable_writable_file_system_and_parent_with_implicit_enable_writable_file_system() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "enable_writable_file_system": true,
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        enable_writable_file_system: true,
                        parent: container_container_parent!("parent", all, -enable_writable_file_system),
                    },
                )
            }

            #[test]
            fn enable_writable_file_system_and_parent_with_explicit_enable_writable_file_system() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "enable_writable_file_system": true,
                        "parent": {
                            "name": "parent",
                            "use": [ "enable_writable_file_system" ]
                        }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `enable_writable_file_system` cannot be set if `parent` with an explicit `use` of ",
                        "`enable_writable_file_system` is also specified",
                    ),
                )
            }

            #[test]
            fn enable_writable_file_system_and_parent_without_enable_writable_file_system() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "enable_writable_file_system": true,
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        enable_writable_file_system: true,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod mounts {
            use super::*;

            #[test]
            fn mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "mounts": [
                            { "type": "tmp", "mount_point": "/tmp" },
                            { "type": "bind", "mount_point": "/bind", "local_path": "/a" },
                            { "type": "bind", "mount_point": "/bind2", "local_path": "/b", "read_only": false },
                            { "type": "bind", "mount_point": "/bind3", "local_path": "/c", "read_only": true }
                        ]
                    }"#})
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
            fn added_mounts_and_mounts() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }],
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#})
                    .unwrap_err(),
                    "field `added_mounts` cannot be set with `mounts` field",
                );
            }

            #[test]
            fn added_mounts_and_parent_with_implicit_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        mounts: [tmp_mount!("/tmp")],
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn added_mounts_and_parent_with_explicit_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "mounts" ]
                        },
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        mounts: [tmp_mount!("/tmp")],
                        parent: container_container_parent!("parent", mounts),
                    },
                );
            }

            #[test]
            fn added_mounts_and_parent_without_mounts() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_mounts` requires `parent` being specified with a ",
                        "`use` of `mounts` (try `mounts` instead)",
                    ),
                );
            }

            #[test]
            fn added_mounts_and_no_parent() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "added_mounts": [{ "type": "tmp", "mount_point": "/tmp" }]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `added_mounts` cannot be set without ",
                        "`parent` also being specified (try `mounts` instead)",
                    ),
                );
            }

            #[test]
            fn empty_added_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "added_mounts": []
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_implicit_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                );
            }

            #[test]
            fn parent_with_explicit_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "mounts" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", mounts),
                    },
                );
            }

            #[test]
            fn mounts_and_parent_with_implicit_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent",
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        mounts: [proc_mount!("/proc")],
                        parent: container_container_parent!("parent", all, -mounts),
                    },
                );
            }

            #[test]
            fn mounts_and_parent_with_explicit_mounts() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "mounts" ]
                        },
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }]
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `mounts` cannot be set if `parent` with an explicit `use` of ",
                        "`mounts` is also specified (try `added_mounts` instead)",
                    ),
                );
            }

            #[test]
            fn mounts_and_parent_without_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        },
                        "mounts": [{ "type": "proc", "mount_point": "/proc" }]
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        mounts: [proc_mount!("/proc")],
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn no_mounts_and_no_parent() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                    },
                );
            }

            #[test]
            fn no_mounts_and_parent_without_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", environment),
                    },
                );
            }

            #[test]
            fn empty_mounts() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "mounts": []
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                    },
                );
            }
        }

        mod network {
            use super::*;

            #[test]
            fn network() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "network": "loopback"
                    }"#})
                    .unwrap(),
                    job_spec!("/bin/sh", network: JobNetwork::Loopback),
                )
            }

            #[test]
            fn parent_with_implicit_network() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_network() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "network" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", network),
                    },
                )
            }

            #[test]
            fn network_and_parent_with_implicit_network() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "network": "loopback",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        network: JobNetwork::Loopback,
                        parent: container_container_parent!("parent", all, -network),
                    },
                )
            }

            #[test]
            fn network_and_parent_with_explicit_network() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "network": "loopback",
                        "parent": {
                            "name": "parent",
                            "use": [ "network" ]
                        }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `network` cannot be set if `parent` with an explicit `use` of ",
                        "`network` is also specified",
                    ),
                )
            }

            #[test]
            fn network_and_parent_without_network() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "network": "loopback",
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        network: JobNetwork::Loopback,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod user {
            use super::*;

            #[test]
            fn user() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "user": 1234
                    }"#})
                    .unwrap(),
                    job_spec!("/bin/sh", user: 1234),
                )
            }

            #[test]
            fn parent_with_implicit_user() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_user() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "user" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", user),
                    },
                )
            }

            #[test]
            fn user_and_parent_with_implicit_user() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "user": 101,
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        user: 101,
                        parent: container_container_parent!("parent", all, -user),
                    },
                )
            }

            #[test]
            fn user_and_parent_with_explicit_user() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "user": 101,
                        "parent": {
                            "name": "parent",
                            "use": [ "user" ]
                        }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `user` cannot be set if `parent` with an explicit `use` of ",
                        "`user` is also specified",
                    ),
                )
            }

            #[test]
            fn user_and_parent_without_user() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "user": 101,
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        user: 101,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }

        mod group {
            use super::*;
            #[test]
            fn group() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "layers": [ { "tar": "1" } ],
                        "group": 4321
                    }"#})
                    .unwrap(),
                    job_spec!("/bin/sh", layers: [tar_layer!("1")], group: 4321),
                )
            }

            #[test]
            fn parent_with_implicit_group() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", all),
                    },
                )
            }

            #[test]
            fn parent_with_explicit_group() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "parent": {
                            "name": "parent",
                            "use": [ "group" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        parent: container_container_parent!("parent", group),
                    },
                )
            }

            #[test]
            fn group_and_parent_with_implicit_group() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "group": 101,
                        "parent": "parent"
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        group: 101,
                        parent: container_container_parent!("parent", all, -group),
                    },
                )
            }

            #[test]
            fn group_and_parent_with_explicit_group() {
                assert_error(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "group": 101,
                        "parent": {
                            "name": "parent",
                            "use": [ "group" ]
                        }
                    }"#})
                    .unwrap_err(),
                    concat!(
                        "field `group` cannot be set if `parent` with an explicit `use` of ",
                        "`group` is also specified",
                    ),
                )
            }

            #[test]
            fn group_and_parent_without_group() {
                assert_eq!(
                    parse_job_spec(indoc! {r#"{
                        "program": "/bin/sh",
                        "group": 101,
                        "parent": {
                            "name": "parent",
                            "use": [ "environment" ]
                        }
                    }"#})
                    .unwrap(),
                    job_spec! {
                        "/bin/sh",
                        group: 101,
                        parent: container_container_parent!("parent", environment),
                    },
                )
            }
        }
    }
}
