use super::{
    directive::{Directive, DirectiveContainer, DirectiveContainerAccumulate},
    TestMetadata,
};
use crate::TestFilter;
use anyhow::{Context as _, Result};
use maelstrom_client::{
    spec::{ContainerParent, ContainerSpec, ImageRef},
    ProjectDir,
};
use maelstrom_util::{fs::Fs, root::Root, template::TemplateVars};
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

impl<TestFilterT> Default for Store<TestFilterT> {
    fn default() -> Self {
        Self {
            directives: Default::default(),
            containers: Default::default(),
        }
    }
}

impl<TestFilterT> Store<TestFilterT> {
    pub fn with_default_directive(mut self) -> Self {
        self.directives = [Directive::default()].into_iter().collect();
        self
    }
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
    pub fn get_metadata_for_test(
        &self,
        package: &TestFilterT::Package,
        artifact: &TestFilterT::ArtifactKey,
        case: (&str, &TestFilterT::CaseMetadata),
    ) -> Result<TestMetadata> {
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
            .try_fold(TestMetadata::default(), |m, d| m.try_fold(d))
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

    pub fn load(
        log: slog::Logger,
        project_dir: impl AsRef<Root<ProjectDir>>,
        test_metadata_file_name: &str,
        default_test_metadata_contents: &str,
        vars: &TemplateVars,
    ) -> Result<Self> {
        struct MaelstromTestTomlFile;
        let path = project_dir
            .as_ref()
            .join::<MaelstromTestTomlFile>(test_metadata_file_name);

        let mut result = if let Some(contents) = Fs::new().read_to_string_if_exists(&path)? {
            Self::from_str(&contents).with_context(|| format!("parsing {}", path.display()))?
        } else {
            slog::debug!(
                log,
                "no test metadata configuration found, using default";
                "search_path" => ?path,
            );
            Self::from_str(default_test_metadata_contents)
                .expect("embedded default test metadata TOML is valid")
        };

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
}
