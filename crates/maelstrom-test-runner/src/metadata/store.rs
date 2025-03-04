use super::{
    directive::{Directive, DirectiveContainer, DirectiveContainerAugment},
    Metadata, MetadataInternal,
};
use crate::TestFilter;
use anyhow::{anyhow, Result};
use maelstrom_client::spec::{
    ContainerParent, ContainerRef, ContainerSpec, ContainerUse, ImageRef, ImageUse,
};
use maelstrom_util::template::TemplateVariables;
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
    pub fn load(contents: &str, vars: &TemplateVariables) -> Result<Self> {
        let mut contents: FileContents<TestFilterT> = toml::from_str(contents)?;

        for directive in &mut contents.directives {
            match &mut directive.container {
                DirectiveContainer::Override(ContainerSpec { layers, .. }) => {
                    for layer in layers {
                        layer.replace_template_variables(vars)?;
                    }
                }
                DirectiveContainer::Augment(DirectiveContainerAugment {
                    layers,
                    added_layers,
                    ..
                }) => {
                    if let Some(layers) = layers {
                        for layer in layers {
                            layer.replace_template_variables(vars)?;
                        }
                    }
                    for added_layer in added_layers {
                        added_layer.replace_template_variables(vars)?;
                    }
                }
            }
        }

        for container in contents.containers.values_mut() {
            let ContainerSpec { layers, .. } = container;
            for layer in layers {
                layer.replace_template_variables(vars)?;
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
        let metadata = self
            .directives
            .iter()
            .filter(|directive| match directive {
                Directive { filter: None, .. } => true,
                Directive {
                    filter: Some(filter),
                    ..
                } => filter
                    .filter(package, Some(artifact), Some(case))
                    .expect("should have case"),
            })
            .fold(MetadataInternal::default(), MetadataInternal::fold);
        let uses_image_layers = self.parent_uses_image_layers(&metadata.container.parent)?;
        Ok(Metadata::new(metadata, uses_image_layers))
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

    fn parent_uses_image_layers(&self, parent: &Option<ContainerParent>) -> Result<bool> {
        self.parent_uses_image_layers_internal(parent, vec![])
    }

    fn parent_uses_image_layers_internal<'a>(
        &'a self,
        parent: &'a Option<ContainerParent>,
        mut ancestors: Vec<&'a str>,
    ) -> Result<bool> {
        match parent {
            None => Ok(false),
            Some(ContainerParent::Image(image_ref)) => {
                Ok(image_ref.r#use.contains(ImageUse::Layers))
            }
            Some(ContainerParent::Container(container_ref))
                if !container_ref.r#use.contains(ContainerUse::Layers) =>
            {
                Ok(false)
            }
            Some(ContainerParent::Container(ContainerRef { name, .. })) => {
                if ancestors.iter().any(|ancestor| ancestor == name) {
                    ancestors.push(name);
                    Err(anyhow!("ancestor loop: {}", ancestors.join(" -> ")))
                } else {
                    ancestors.push(name);
                    if let Some(grandparent) = self.containers.get(name) {
                        self.parent_uses_image_layers_internal(&grandparent.parent, ancestors)
                    } else {
                        Err(anyhow!("could not find container parent: {name}"))
                    }
                }
            }
        }
    }

    pub fn containers(&self) -> impl Iterator<Item = (&String, &ContainerSpec)> {
        self.containers.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metadata::{
            directive::{augment_directive, override_directive},
            metadata,
        },
        NoCaseMetadata, SimpleFilter,
    };
    use indoc::indoc;
    use maelstrom_base::JobNetwork;
    use maelstrom_client::{
        container_container_parent, container_spec, image_container_parent, image_ref,
        tar_layer_spec,
    };
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
                        override_directive! {
                            filter: "package = \"package1\"",
                            parent: image_container_parent!("image1", all),
                            network: JobNetwork::Disabled,
                        },
                        augment_directive! {
                            network: JobNetwork::Loopback,
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
                        override_directive! {
                            filter: "package = \"package1\"",
                            parent: image_container_parent!("image1", all),
                            network: JobNetwork::Disabled,
                        },
                        augment_directive! {
                            network: JobNetwork::Loopback,
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
    fn template_variables() {
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
            &TemplateVariables::new([("foo", "bar")]),
        )
        .unwrap();
        assert_eq!(
            store.directives,
            vec![
                override_directive! {
                    filter: "package = \"package1\"",
                    parent: image_container_parent!("image1", all),
                    layers: [tar_layer_spec!("bar.tar")],
                    network: JobNetwork::Disabled,
                },
                augment_directive! {
                    network: JobNetwork::Loopback,
                    added_layers: [tar_layer_spec!("bar.tar")],
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
                .unwrap(),
            metadata! {
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test0", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                network: JobNetwork::Loopback,
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                network: JobNetwork::Loopback,
                user: 101,
                include_shared_libraries: true,
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
                .unwrap(),
            metadata! {
                group: 202,
                enable_writable_file_system: true,
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test0", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                group: 202,
                enable_writable_file_system: true,
                network: JobNetwork::Loopback,
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                group: 202,
                enable_writable_file_system: true,
                network: JobNetwork::Loopback,
                user: 101,
                include_shared_libraries: true,
            },
        );
    }

    #[test]
    fn get_metadata_for_test_include_shared_libraries() {
        let store = Store::<SimpleFilter>::load(
            indoc! {r#"
                [containers.parent1]
                image = "image"

                [containers.parent2]

                [[directives]]
                filter = "package = \"package1\""
                parent = "parent1"

                [[directives]]
                filter = "package = \"package2\""
                parent = "parent2"

                [[directives]]
                filter = "name = \"test2\""
                include_shared_libraries = false

                [[directives]]
                filter = "name = \"test3\""
                include_shared_libraries = true
            "#},
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                parent: container_container_parent!("parent1", all),
                include_shared_libraries: false,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test2", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                parent: container_container_parent!("parent1", all),
                include_shared_libraries: false,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package1".into(),
                    &"package1".into(),
                    ("test3", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                parent: container_container_parent!("parent1", all),
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package2".into(),
                    &"package2".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                parent: container_container_parent!("parent2", all),
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package2".into(),
                    &"package2".into(),
                    ("test2", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                parent: container_container_parent!("parent2", all),
                include_shared_libraries: false,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package2".into(),
                    &"package2".into(),
                    ("test3", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                parent: container_container_parent!("parent2", all),
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package3".into(),
                    &"package3".into(),
                    ("test1", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                include_shared_libraries: true,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package3".into(),
                    &"package3".into(),
                    ("test2", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                include_shared_libraries: false,
            },
        );
        assert_eq!(
            store
                .get_metadata_for_test(
                    &"package3".into(),
                    &"package3".into(),
                    ("test3", &NoCaseMetadata)
                )
                .unwrap(),
            metadata! {
                include_shared_libraries: true,
            },
        );
    }

    mod parent_uses_image_layers {
        use super::*;

        #[test]
        fn none() {
            let store = Store::<SimpleFilter>::load("", &Default::default()).unwrap();
            assert!(!store.parent_uses_image_layers(&None).unwrap());
        }

        #[test]
        fn image_parent_without_use_of_layers() {
            let store = Store::<SimpleFilter>::load("", &Default::default()).unwrap();
            assert!(!store
                .parent_uses_image_layers(&Some(image_container_parent!("foo", all, -layers)))
                .unwrap());
        }

        #[test]
        fn image_parent() {
            let store = Store::<SimpleFilter>::load("", &Default::default()).unwrap();
            assert!(store
                .parent_uses_image_layers(&Some(image_container_parent!("foo", all)))
                .unwrap());
        }

        #[test]
        fn container_parent_without_use_of_layers() {
            let store = Store::<SimpleFilter>::load("", &Default::default()).unwrap();
            assert!(!store
                .parent_uses_image_layers(&Some(container_container_parent!("foo", all, -layers)))
                .unwrap());
        }

        #[test]
        fn container_parent_no_grandparent() {
            let store = Store::<SimpleFilter>::load(
                indoc! {r#"
                    [containers.parent]
                "#},
                &Default::default(),
            )
            .unwrap();
            assert!(!store
                .parent_uses_image_layers(&Some(container_container_parent!("parent", all)))
                .unwrap());
        }

        #[test]
        fn container_parent_image_grandparent() {
            let store = Store::<SimpleFilter>::load(
                indoc! {r#"
                    [containers.parent]
                    image = "grandparent"
                "#},
                &Default::default(),
            )
            .unwrap();
            assert!(store
                .parent_uses_image_layers(&Some(container_container_parent!("parent", all)))
                .unwrap());
        }

        #[test]
        fn container_parent_image_grandparent_without_use_of_layers() {
            let store = Store::<SimpleFilter>::load(
                indoc! {r#"
                    [containers.parent]
                    image.name = "grandparent"
                    image.use = ["environment"]
                "#},
                &Default::default(),
            )
            .unwrap();
            assert!(!store
                .parent_uses_image_layers(&Some(container_container_parent!("parent", all)))
                .unwrap());
        }

        #[test]
        fn unknown_container_parent() {
            let store = Store::<SimpleFilter>::load("", &Default::default()).unwrap();
            assert!(store
                .parent_uses_image_layers(&Some(container_container_parent!("parent", all)))
                .unwrap_err()
                .to_string()
                .contains("could not find container parent: parent"));
        }

        #[test]
        fn container_parent_cycle() {
            let store = Store::<SimpleFilter>::load(
                indoc! {r#"
                    [containers.parent1]
                    parent = "parent2"
                    [containers.parent2]
                    parent = "parent1"
                "#},
                &Default::default(),
            )
            .unwrap();
            assert!(store
                .parent_uses_image_layers(&Some(container_container_parent!("parent1", all)))
                .unwrap_err()
                .to_string()
                .contains("ancestor loop: parent1 -> parent2 -> parent1"));
        }

        #[test]
        fn container_parent_deep_with_image_use() {
            let store = Store::<SimpleFilter>::load(
                indoc! {r#"
                    [containers.ancestor1]
                    parent = "ancestor2"
                    [containers.ancestor2]
                    parent = "ancestor3"
                    [containers.ancestor3]
                    image = "image"
                "#},
                &Default::default(),
            )
            .unwrap();
            assert!(store
                .parent_uses_image_layers(&Some(container_container_parent!("ancestor1", all)))
                .unwrap());
        }

        #[test]
        fn container_parent_deep_without_image_use() {
            let store = Store::<SimpleFilter>::load(
                indoc! {r#"
                    [containers.ancestor1]
                    parent = "ancestor2"
                    [containers.ancestor2]
                    parent.name = "ancestor3"
                    parent.use = ["environment"]
                    [containers.ancestor3]
                    image = "image"
                "#},
                &Default::default(),
            )
            .unwrap();
            assert!(!store
                .parent_uses_image_layers(&Some(container_container_parent!("ancestor1", all)))
                .unwrap());
        }
    }
}
