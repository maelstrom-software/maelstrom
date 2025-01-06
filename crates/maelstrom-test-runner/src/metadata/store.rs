use super::{
    directive::{Directive, DirectiveContainer, DirectiveContainerAccumulate},
    Metadata,
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

#[cfg(test)]
impl<TestFilterT> Default for Store<TestFilterT> {
    fn default() -> Self {
        Self {
            directives: Default::default(),
            containers: Default::default(),
        }
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

    #[cfg(test)]
    fn with_default_directive() -> Self {
        Self {
            directives: vec![Directive::default()],
            containers: Default::default(),
        }
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
    use maelstrom_client::ProjectDir;
    use maelstrom_util::{fs::Fs, root::RootBuf, template::TemplateVars};
    use slog::Drain as _;
    use std::{
        io,
        sync::{Arc, Mutex},
    };
    use toml::de::Error as TomlError;

    #[derive(Clone, Default)]
    struct InMemoryLogOutput(Arc<Mutex<Vec<u8>>>);

    impl InMemoryLogOutput {
        fn lines(&self) -> Vec<serde_json::Value> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .split('\n')
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect()
        }
    }

    impl io::Write for InMemoryLogOutput {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn load_test(t: &tempfile::TempDir) -> (Store<SimpleFilter>, Vec<serde_json::Value>) {
        let project_dir = RootBuf::<ProjectDir>::new(t.path().to_path_buf());
        let log_output = InMemoryLogOutput::default();
        let log = slog::Logger::root(
            Mutex::new(slog_json::Json::default(log_output.clone())).map(slog::Fuse),
            slog::o!(),
        );
        let res = Store::<SimpleFilter>::load(
            log,
            &project_dir,
            "simple-test.toml",
            "[[directives]]",
            &TemplateVars::default(),
        )
        .unwrap();

        let log_lines = log_output.lines();

        (res, log_lines)
    }

    #[test]
    fn load_no_file_found() {
        let t = tempfile::tempdir().unwrap();
        let (res, log_lines) = load_test(&t);

        assert_eq!(res, Store::with_default_directive());
        assert_eq!(log_lines.len(), 1, "{log_lines:?}");
        assert_eq!(
            log_lines[0]["msg"],
            "no test metadata configuration found, using default"
        );
        assert_eq!(log_lines[0]["level"], "DEBG");
    }

    #[test]
    fn load_expected_file() {
        let fs = Fs::new();
        let t = tempfile::tempdir().unwrap();
        fs.write(t.path().join("simple-test.toml"), "[[directives]]")
            .unwrap();

        let (res, log_lines) = load_test(&t);

        assert_eq!(res, Store::with_default_directive());
        assert_eq!(log_lines.len(), 0, "{log_lines:?}");
    }

    #[test]
    fn default() {
        assert_eq!(
            Store::<SimpleFilter>::default()
                .get_metadata_for_test(&"mod".into(), &"mod".into(), ("foo", &NoCaseMetadata))
                .unwrap(),
            Metadata::default(),
        );
    }

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
