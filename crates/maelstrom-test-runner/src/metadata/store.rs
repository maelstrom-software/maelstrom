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
    collections::HashMap,
    fmt::Display,
    str::{self, FromStr},
};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Store<TestFilterT> {
    #[serde(bound(deserialize = "TestFilterT: FromStr, TestFilterT::Err: Display"))]
    directives: Vec<Directive<TestFilterT>>,
    #[serde(default)]
    containers: HashMap<String, ContainerSpec>,
}

impl<TestFilterT: TestFilter> FromStr for Store<TestFilterT>
where
    TestFilterT::Err: Display,
{
    type Err = anyhow::Error;

    fn from_str(contents: &str) -> Result<Self> {
        Ok(toml::from_str(contents)?)
    }
}

impl<TestFilterT: TestFilter> Store<TestFilterT>
where
    TestFilterT::Err: Display,
{
    pub fn load(contents: &str, vars: &TemplateVars) -> Result<Self> {
        let mut result = Self::from_str(contents)?;

        for directive in &mut result.directives {
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

        Ok(result)
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

    pub fn get_all_images(&self) -> Vec<ImageRef> {
        self.directives
            .iter()
            .filter_map(|directive| {
                if let DirectiveContainer::Override(ContainerSpec {
                    parent: Some(ContainerParent::Image(image)),
                    ..
                }) = &directive.container
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
    use anyhow::Error;
    use toml::de::Error as TomlError;

    #[test]
    fn include_shared_libraries_defaults() {
        let all = Store::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "and = [ { package = \"package1\" }, { name = \"test1\" } ]"
            layers = [{ tar = "layer1" }]

            [[directives]]
            filter = "and = [ { package = \"package1\" }, { name = \"test2\" } ]"
            image = "foo"
            "#,
        )
        .unwrap();
        assert!(all
            .get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .include_shared_libraries());
        assert!(!all
            .get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .include_shared_libraries());
        assert!(all
            .get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .include_shared_libraries());
    }

    #[test]
    fn include_shared_libraries() {
        let all = Store::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            include_shared_libraries = false

            [[directives]]
            filter = "package = \"package1\""
            include_shared_libraries = true
            layers = [{ tar = "layer1" }]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            layers = []
            "#,
        )
        .unwrap();
        assert!(all
            .get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .include_shared_libraries());
        assert!(all
            .get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .include_shared_libraries());
        assert!(!all
            .get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .include_shared_libraries());
    }

    fn assert_toml_error(err: Error, expected: &str) {
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(message.starts_with(expected), "message: {message}");
    }

    #[test]
    fn bad_field_in_all_metadata() {
        assert_toml_error(
            Store::<SimpleFilter>::from_str(
                r#"
                [not_a_field]
                foo = "three"
                "#,
            )
            .unwrap_err(),
            "unknown field `not_a_field`, expected `directives`",
        );
    }
}
