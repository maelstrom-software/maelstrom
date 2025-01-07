use super::{
    directive::{Directive, DirectiveContainer, DirectiveContainerAccumulate},
    Metadata,
};
use crate::TestFilter;
use anyhow::Result;
use maelstrom_client::spec::{ContainerParent, ContainerSpec, ImageRef};
use maelstrom_util::template::TemplateVars;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::{self, FromStr},
};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct FileContents<TestFilterT> {
    #[serde(bound(deserialize = "TestFilterT: FromStr, TestFilterT::Err: Display"))]
    #[serde(default = "Vec::default")]
    directives: Vec<Directive<TestFilterT>>,
    #[serde(default)]
    containers: HashMap<String, ContainerSpec>,
}

pub struct Store<TestFilterT> {
    directives: Vec<Directive<TestFilterT>>,
    containers: HashMap<String, ContainerSpec>,
}

impl<TestFilterT: TestFilter> Store<TestFilterT>
where
    TestFilterT::Err: Display,
{
    pub fn load(contents: &str, vars: &TemplateVars) -> Result<Self> {
        let mut contents: FileContents<TestFilterT> = toml::from_str(contents)?;

        for directive in &mut contents.directives {
            match &mut directive.container {
                DirectiveContainer::Override(ContainerSpec { layers, .. }) => {
                    for layer in layers {
                        layer.replace_template_vars(vars)?;
                    }
                }
                DirectiveContainer::Accumulate(DirectiveContainerAccumulate {
                    layers,
                    added_layers,
                    ..
                }) => {
                    if let Some(layers) = layers {
                        for layer in layers {
                            layer.replace_template_vars(vars)?;
                        }
                    }
                    if let Some(added_layers) = added_layers {
                        for added_layer in added_layers {
                            added_layer.replace_template_vars(vars)?;
                        }
                    }
                }
            }
        }

        for container in contents.containers.values_mut() {
            let ContainerSpec { layers, .. } = container;
            for layer in layers {
                layer.replace_template_vars(vars)?;
            }
        }

        Ok(Self {
            directives: contents.directives,
            containers: contents.containers,
        })
    }

    pub fn get_metadata_for_test(
        &self,
        package: &TestFilterT::Package,
        artifact: &TestFilterT::ArtifactKey,
        case: (&str, &TestFilterT::CaseMetadata),
    ) -> Result<Metadata> {
        self.directives
            .iter()
            .filter(|directive| match directive {
                Directive {
                    filter: Some(filter),
                    ..
                } => filter
                    .filter(package, Some(artifact), Some(case))
                    .expect("should have case"),
                Directive { filter: None, .. } => true,
            })
            .try_fold(Metadata::default(), |m, d| m.try_fold(d))
    }

    pub fn get_all_images(&self) -> HashSet<ImageRef> {
        self.directives
            .iter()
            .filter_map(|directive| {
                if let DirectiveContainer::Override(container) = &directive.container {
                    Some(container)
                } else {
                    None
                }
            })
            .chain(self.containers.values())
            .filter_map(|container| {
                if let ContainerSpec {
                    parent: Some(ContainerParent::Image(image)),
                    ..
                } = container
                {
                    Some(image.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn containers(&self) -> impl Iterator<Item = (&String, &ContainerSpec)> {
        self.containers.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoCaseMetadata, SimpleFilter};
    use indoc::indoc;
    use maelstrom_base::JobNetwork;
    use maelstrom_client::{container_spec, image_container_parent, image_ref, tar_layer_spec};
    use maplit::{hashmap, hashset};

    mod parse {
        use super::*;

        #[track_caller]
        fn parse_toml(file: &str) -> FileContents<SimpleFilter> {
            toml::from_str(file).unwrap()
        }

        #[track_caller]
        fn parse_error_toml(file: &str) -> String {
            format!(
                "{}",
                toml::from_str::<FileContents<SimpleFilter>>(file).unwrap_err()
            )
            .trim_end()
            .into()
        }

        #[test]
        fn empty_file() {
            assert_eq!(
                parse_toml(""),
                FileContents {
                    directives: Default::default(),
                    containers: Default::default(),
                }
            );
        }

        #[test]
        fn unknown_field() {
            assert!(parse_error_toml(indoc! {r#"
                [not_a_field]
                foo = "three"
            "#})
            .contains("unknown field `not_a_field`"));
        }

        #[test]
        fn only_directives() {
            assert_eq!(
                parse_toml(indoc! {r#"
                    [[directives]]
                    filter = "package = \"package1\""
                    image = "image1"
                    network = "disabled"

                    [[directives]]
                    network = "loopback"
                "#}),
                FileContents {
                    directives: vec![
                        Directive {
                            filter: Some(SimpleFilter::Package("package1".into())),
                            container: DirectiveContainer::Override(container_spec! {
                                parent: image_container_parent!("image1", all),
                                network: JobNetwork::Disabled,
                            }),
                            ..Default::default()
                        },
                        Directive {
                            container: DirectiveContainer::Accumulate(
                                DirectiveContainerAccumulate {
                                    network: Some(JobNetwork::Loopback),
                                    ..Default::default()
                                }
                            ),
                            ..Default::default()
                        },
                    ],
                    containers: Default::default(),
                },
            );
        }

        #[test]
        fn only_containers() {
            assert_eq!(
                parse_toml(indoc! {r#"
                    [containers.container1]
                    image = "image1"
                    network = "disabled"

                    [containers."foo.bar.baz"]
                    network = "loopback"
                "#}),
                FileContents {
                    directives: Default::default(),
                    containers: hashmap! {
                        "container1".into() => container_spec! {
                            parent: image_container_parent!("image1", all),
                            network: JobNetwork::Disabled,
                        },
                        "foo.bar.baz".into() => container_spec! {
                            network: JobNetwork::Loopback,
                        },
                    },
                },
            );
        }

        #[test]
        fn directives_and_containers() {
            assert_eq!(
                parse_toml(indoc! {r#"
                    [[directives]]
                    filter = "package = \"package1\""
                    image = "image1"
                    network = "disabled"

                    [containers.container1]
                    image = "image1"
                    network = "disabled"

                    [[directives]]
                    network = "loopback"

                    [containers."foo.bar.baz"]
                    network = "loopback"
                "#}),
                FileContents {
                    directives: vec![
                        Directive {
                            filter: Some(SimpleFilter::Package("package1".into())),
                            container: DirectiveContainer::Override(container_spec! {
                                parent: image_container_parent!("image1", all),
                                network: JobNetwork::Disabled,
                            }),
                            ..Default::default()
                        },
                        Directive {
                            container: DirectiveContainer::Accumulate(
                                DirectiveContainerAccumulate {
                                    network: Some(JobNetwork::Loopback),
                                    ..Default::default()
                                }
                            ),
                            ..Default::default()
                        },
                    ],
                    containers: hashmap! {
                        "container1".into() => container_spec! {
                            parent: image_container_parent!("image1", all),
                            network: JobNetwork::Disabled,
                        },
                        "foo.bar.baz".into() => container_spec! {
                            network: JobNetwork::Loopback,
                        },
                    },
                },
            );
        }
    }

    #[test]
    fn template_vars() {
        let store = Store::<SimpleFilter>::load(
            indoc! {r#"
                [[directives]]
                filter = "package = \"package1\""
                image = "image1"
                network = "disabled"
                added_layers = [ { tar = "<foo>.tar" } ]

                [[directives]]
                network = "loopback"
                added_layers = [ { tar = "<foo>.tar" } ]

                [containers.container1]
                image = "image1"
                added_layers = [ { tar = "<foo>.tar" } ]
            "#},
            &TemplateVars::new([("foo", "bar")]),
        )
        .unwrap();
        assert_eq!(
            store.directives,
            vec![
                Directive {
                    filter: Some(SimpleFilter::Package("package1".into())),
                    container: DirectiveContainer::Override(container_spec! {
                        parent: image_container_parent!("image1", all),
                        layers: [tar_layer_spec!("bar.tar")],
                        network: JobNetwork::Disabled,
                    }),
                    ..Default::default()
                },
                Directive {
                    container: DirectiveContainer::Accumulate(DirectiveContainerAccumulate {
                        network: Some(JobNetwork::Loopback),
                        added_layers: Some(vec![tar_layer_spec!("bar.tar")]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
        );
        assert_eq!(
            store.containers,
            hashmap! {
                "container1".into() => container_spec! {
                    parent: image_container_parent!("image1", all),
                    layers: [tar_layer_spec!("bar.tar")],
                },
            },
        );
    }

    #[test]
    fn get_all_images() {
        let store = Store::<SimpleFilter>::load(
            indoc! {r#"
                [[directives]]
                network = "loopback"

                [[directives]]
                filter = "package = \"package1\""
                image.name = "image1"
                image.use = ["layers"]
                network = "disabled"

                [containers.container1]
                image = "image1"
                network = "loopback"

                [containers.container2]
                image = "image2"
                network = "loopback"
            "#},
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            store.get_all_images(),
            hashset! {
                image_ref!("image1", all),
                image_ref!("image1", layers),
                image_ref!("image2", all),
            },
        );
    }

    #[test]
    fn get_metadata_for_test_all_have_filters() {
        let store = Store::<SimpleFilter>::load(
            indoc! {r#"
                [[directives]]
                filter = "package = \"package1\""
                network = "loopback"

                [[directives]]
                filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
                user = 101
            "#},
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package0".into(),
                    &"package0".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap()
                .container,
            container_spec! {},
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test0", &NoCaseMetadata)
                )
                .unwrap()
                .container,
            container_spec! { network: JobNetwork::Loopback },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap()
                .container,
            container_spec! {
                network: JobNetwork::Loopback,
                user: 101,
            },
        );
    }

    #[test]
    fn get_metadata_for_test_not_all_have_filters() {
        let store = Store::<SimpleFilter>::load(
            indoc! {r#"
                [[directives]]
                group = 202

                [[directives]]
                filter = "package = \"package1\""
                network = "loopback"

                [[directives]]
                enable_writable_file_system = true

                [[directives]]
                filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
                user = 101
            "#},
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package0".into(),
                    &"package0".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap()
                .container,
            container_spec! {
                group: 202,
                enable_writable_file_system: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test0", &NoCaseMetadata)
                )
                .unwrap()
                .container,
            container_spec! {
                group: 202,
                enable_writable_file_system: true,
                network: JobNetwork::Loopback,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap()
                .container,
            container_spec! {
                group: 202,
                enable_writable_file_system: true,
                network: JobNetwork::Loopback,
                user: 101,
            },
        );
    }
}
