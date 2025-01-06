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
struct FileContents<TestFilterT> {
    #[serde(bound(deserialize = "TestFilterT: FromStr, TestFilterT::Err: Display"))]
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
    use crate::SimpleFilter;
    use anyhow::Error;
    use toml::de::Error as TomlError;

    fn assert_toml_error(err: Error, expected: &str) {
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(message.starts_with(expected), "message: {message}");
    }

    #[test]
    fn bad_field_in_all_metadata() {
        assert_toml_error(
            toml::from_str::<FileContents<SimpleFilter>>(
                r#"
                [not_a_field]
                foo = "three"
                "#,
            )
            .map_err(Error::from)
            .unwrap_err(),
            "unknown field `not_a_field`, expected `directives`",
        );
    }
}
