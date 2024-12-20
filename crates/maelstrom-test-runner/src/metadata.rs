mod directive;

use crate::TestFilter;
use anyhow::{Context as _, Result};
use directive::{Directive, DirectiveContainer, DirectiveContainerAccumulate};
use maelstrom_base::Timeout;
use maelstrom_client::{
    spec::{ContainerParent, ContainerSpec, EnvironmentSpec, ImageRef},
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
pub struct AllMetadata<TestFilterT> {
    #[serde(bound(deserialize = "TestFilterT: FromStr, TestFilterT::Err: Display"))]
    directives: Vec<Directive<TestFilterT>>,
    #[serde(default)]
    containers: HashMap<String, ContainerSpec>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TestMetadata {
    pub container: ContainerSpec,
    include_shared_libraries: Option<bool>,
    pub timeout: Option<Timeout>,
    pub ignore: bool,
}

impl TestMetadata {
    /// Return whether to include a layer of shared library dependencies.
    ///
    /// The logic here is that if they explicitly set the value to something, we should return
    /// that. Otherwise, we should see if they specified an image. If they specified an image, we
    /// can assume that the image has shared libraries, and we shouldn't push shared libraries on
    /// top of it. Otherwise, if they didn't provide an image, then they probably don't want to
    /// have to provide a layer with shared libraries in it.
    pub fn include_shared_libraries(&self) -> bool {
        match self.include_shared_libraries {
            Some(val) => val,
            None => self.container.parent.is_none(),
        }
    }

    fn try_fold<TestFilterT>(mut self, directive: &Directive<TestFilterT>) -> Result<Self> {
        let rhs = directive;

        self.container = match &rhs.container {
            DirectiveContainer::Override(container) => container.clone(),
            DirectiveContainer::Accumulate(rhs) => {
                let mut layers = rhs.layers.clone().unwrap_or(self.container.layers);
                layers.extend(rhs.added_layers.iter().flatten().cloned());

                let mut environment = self.container.environment;
                if let Some(vars) = &rhs.environment {
                    environment.push(EnvironmentSpec {
                        vars: vars.clone(),
                        extend: false,
                    });
                }
                environment.extend(
                    rhs.added_environment
                        .iter()
                        .cloned()
                        .map(|vars| EnvironmentSpec { vars, extend: true }),
                );

                let working_directory = rhs
                    .working_directory
                    .clone()
                    .or(self.container.working_directory);

                let enable_writable_file_system = rhs
                    .enable_writable_file_system
                    .or(self.container.enable_writable_file_system);

                let mut mounts = rhs
                    .mounts
                    .as_ref()
                    .map(|mounts| mounts.iter().cloned().map(Into::into).collect())
                    .unwrap_or(self.container.mounts);
                mounts.extend(rhs.added_mounts.iter().flatten().cloned().map(Into::into));

                let network = rhs.network.or(self.container.network);

                let user = rhs.user.or(self.container.user);

                let group = rhs.group.or(self.container.group);

                ContainerSpec {
                    parent: self.container.parent,
                    layers,
                    environment,
                    working_directory,
                    enable_writable_file_system,
                    mounts,
                    network,
                    user,
                    group,
                }
            }
        };

        self.include_shared_libraries = directive
            .include_shared_libraries
            .or(self.include_shared_libraries);
        self.timeout = directive.timeout.unwrap_or(self.timeout);
        self.ignore = directive.ignore.unwrap_or(self.ignore);

        Ok(self)
    }
}

impl<TestFilterT: TestFilter> FromStr for AllMetadata<TestFilterT>
where
    TestFilterT::Err: Display,
{
    type Err = anyhow::Error;

    fn from_str(contents: &str) -> Result<Self> {
        Ok(toml::from_str(contents)?)
    }
}

impl<TestFilterT: TestFilter> AllMetadata<TestFilterT>
where
    TestFilterT::Err: Display,
{
    pub fn replace_template_vars(&mut self, vars: &TemplateVars) -> Result<()> {
        for directive in &mut self.directives {
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
        Ok(())
    }

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
    ) -> Result<Self> {
        struct MaelstromTestTomlFile;
        let path = project_dir
            .as_ref()
            .join::<MaelstromTestTomlFile>(test_metadata_file_name);
        if let Some(contents) = Fs::new().read_to_string_if_exists(&path)? {
            return Self::from_str(&contents)
                .with_context(|| format!("parsing {}", path.display()));
        }

        slog::debug!(
            log,
            "no test metadata configuration found, using default";
            "search_path" => ?path,
        );
        Ok(Self::from_str(default_test_metadata_contents)
            .expect("embedded default test metadata TOML is valid"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoCaseMetadata, SimpleFilter};
    use anyhow::Error;
    use maelstrom_base::{enum_set, GroupId, JobDevice, JobMount, JobNetwork, UserId};
    use maelstrom_client::image_container_parent;
    use maelstrom_test::{tar_layer, utf8_path_buf};
    use maelstrom_util::root::RootBuf;
    use maplit::btreemap;
    use maplit::hashmap;
    use slog::Drain as _;
    use std::io;
    use std::sync::{Arc, Mutex};
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

    fn load_test(t: &tempfile::TempDir) -> (AllMetadata<SimpleFilter>, Vec<serde_json::Value>) {
        let project_dir = RootBuf::<ProjectDir>::new(t.path().to_path_buf());
        let log_output = InMemoryLogOutput::default();
        let log = slog::Logger::root(
            Mutex::new(slog_json::Json::default(log_output.clone())).map(slog::Fuse),
            slog::o!(),
        );
        let res = AllMetadata::<SimpleFilter>::load(
            log,
            &project_dir,
            "simple-test.toml",
            "[[directives]]",
        )
        .unwrap();

        let log_lines = log_output.lines();

        (res, log_lines)
    }

    #[test]
    fn load_no_file_found() {
        let t = tempfile::tempdir().unwrap();
        let (res, log_lines) = load_test(&t);

        assert_eq!(
            res,
            AllMetadata {
                directives: vec![Directive::default()],
                containers: hashmap! {},
            }
        );
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

        assert_eq!(
            res,
            AllMetadata {
                directives: vec![Directive::default()],
                containers: hashmap! {},
            }
        );
        assert_eq!(log_lines.len(), 0, "{log_lines:?}");
    }

    #[test]
    fn default() {
        assert_eq!(
            AllMetadata::<SimpleFilter> {
                directives: vec![],
                containers: hashmap! {},
            }
            .get_metadata_for_test(&"mod".into(), &"mod".into(), ("foo", &NoCaseMetadata))
            .unwrap(),
            TestMetadata::default(),
        );
    }

    #[test]
    fn include_shared_libraries_defaults() {
        let all = AllMetadata::<SimpleFilter>::from_str(
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
        let all = AllMetadata::<SimpleFilter>::from_str(
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

    #[test]
    fn network() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            network = "disabled"

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            network = "loopback"

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test2\" }]"
            network = "local"
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .network,
            Some(JobNetwork::Loopback),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .network,
            Some(JobNetwork::Local),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .network,
            Some(JobNetwork::Disabled),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .network,
            None,
        );
    }

    #[test]
    fn enable_writable_file_system() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            enable_writable_file_system = true

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            enable_writable_file_system = false
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .enable_writable_file_system,
            Some(false),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .enable_writable_file_system,
            Some(true),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .enable_writable_file_system,
            None,
        );
    }

    #[test]
    fn working_directory() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            include_shared_libraries = false

            [[directives]]
            filter = "package = \"package1\""
            image.name = "rust"
            image.use = ["working_directory"]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            working_directory = "/bar"

            [[directives]]
            filter = "package = \"package3\""
            image.name = "no-working-directory"
            image.use = ["working_directory"]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .working_directory,
            Some(utf8_path_buf!("/bar"))
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent,
            Some(image_container_parent!("rust", working_directory)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .working_directory,
            None,
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .working_directory,
            None,
        );
    }

    #[test]
    fn user() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            user = 101

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            user = 202
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .user,
            Some(UserId::from(202)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .user,
            Some(UserId::from(101)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .user,
            None,
        );
    }

    #[test]
    fn group() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            group = 101

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            group = 202
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .group,
            Some(GroupId::from(202)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .group,
            Some(GroupId::from(101)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .group,
            None,
        );
    }

    #[test]
    fn timeout() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            timeout = 100

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            timeout = 0
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .timeout,
            None,
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .timeout,
            Timeout::new(100),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .timeout,
            None,
        );
    }

    #[test]
    fn layers() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            layers = [{ tar = "layer1" }, { tar = "layer2" }]

            [[directives]]
            filter = "package = \"package1\""
            image.name = "image2"
            image.use = [ "layers" ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            image.name = "image1"
            image.use = [ "layers" ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test2\" }]"
            layers = [{ tar = "layer3" }, { tar = "layer4" }]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test3\" }]"
            image.name = "image3"
            image.use = [ "layers" ]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent,
            Some(image_container_parent!("image1", layers)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![tar_layer!("layer3"), tar_layer!("layer4")],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent,
            Some(image_container_parent!("image2", layers)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent,
            Some(image_container_parent!("image3", layers)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test4", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test4", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent,
            Some(image_container_parent!("image2", layers)),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![tar_layer!("layer1"), tar_layer!("layer2")],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent,
            None,
        );
    }

    #[test]
    fn added_layers() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            added_layers = [{ tar = "added-layer1" }, { tar = "added-layer2" }]

            [[directives]]
            filter = "package = \"package1\""
            layers = [{tar = "layer1" }, { tar = "layer2" }]
            added_layers = [{ tar = "added-layer3" }, { tar = "added-layer4" }]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            added_layers = [{tar = "added-layer5" }, { tar = "added-layer6" }]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![
                tar_layer!("layer1"),
                tar_layer!("layer2"),
                tar_layer!("added-layer3"),
                tar_layer!("added-layer4"),
                tar_layer!("added-layer5"),
                tar_layer!("added-layer6"),
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![
                tar_layer!("layer1"),
                tar_layer!("layer2"),
                tar_layer!("added-layer3"),
                tar_layer!("added-layer4")
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .layers,
            vec![tar_layer!("added-layer1"), tar_layer!("added-layer2")],
        );
    }

    #[test]
    fn environment() {
        let dir1_env = EnvironmentSpec {
            vars: btreemap! {
                "FOO".into() => "$env{FOO}".into(),
                "BAR".into() => "bar".into(),
                "BAZ".into() => "$prev{FOO:-no-prev-foo}".into(),
            },
            extend: false,
        };
        let dir3_env = EnvironmentSpec {
            vars: btreemap! {
                "FOO".into() => "$prev{FOO}".into(),
                "BAR".into() => "$env{BAR}".into(),
                "BAZ".into() => "$prev{BAZ:-no-prev-baz}".into(),
            },
            extend: false,
        };
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            environment = { FOO = "$env{FOO}", BAR = "bar", BAZ = "$prev{FOO:-no-prev-foo}" }

            [[directives]]
            filter = "package = \"package1\""
            image.name = "image1"
            image.use = ["environment"]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            environment = { FOO = "$prev{FOO}", BAR = "$env{BAR}", BAZ = "$prev{BAZ:-no-prev-baz}" }

            [[directives]]
            filter = "package = \"package3\""
            image.name = "no-environment"
            image.use = ["environment"]

            [[directives]]
            filter = "package = \"package4\""
            image.name = "bad-environment"
            image.use = ["environment"]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![dir3_env.clone()]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent
            .unwrap(),
            image_container_parent!("image1", environment),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent
            .unwrap(),
            image_container_parent!("image1", environment),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![dir1_env.clone()]
        );
    }

    #[test]
    fn added_environment() {
        let dir1_env = EnvironmentSpec {
            vars: btreemap! {
                "BAR".into() => "bar".into(),
                "FOO".into() => "foo".into(),
            },
            extend: false,
        };
        let dir1_added_env = EnvironmentSpec {
            vars: btreemap! {
                "BAZ".into() => "$prev{BAZ:-no-prev-baz}".into(),
                "FOO".into() => "prev-$prev{FOO}".into(),
            },
            extend: true,
        };
        let dir2_added_env = EnvironmentSpec {
            vars: btreemap! {
                "BAZ".into() => "$prev{BAZ:-no-prev-baz}".into(),
                "FOO".into() => "$prev{FOO}".into(),
            },
            extend: true,
        };
        let dir3_added_env = EnvironmentSpec {
            vars: btreemap! {
                "FOO".into() => "prev-$prev{FOO}".into(),
                "BAR".into() => "bar".into(),
            },
            extend: true,
        };
        let dir4_env = EnvironmentSpec {
            vars: btreemap! {
                "FOO".into() => "prev-$prev{FOO}".into(),
            },
            extend: false,
        };
        let dir4_added_env = EnvironmentSpec {
            vars: btreemap! {
                "FOO".into() => "prev-$prev{FOO}".into(),
                "BAR".into() => "$env{BAR}".into(),
            },
            extend: true,
        };
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            environment = { FOO = "foo", BAR = "bar" }
            added_environment = { FOO = "prev-$prev{FOO}", BAZ = "$prev{BAZ:-no-prev-baz}" }

            [[directives]]
            filter = "package = \"package1\""
            image.name = "image1"
            image.use = ["environment"]
            added_environment = { FOO = "$prev{FOO}", BAZ = "$prev{BAZ:-no-prev-baz}" }

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            added_environment = { FOO = "prev-$prev{FOO}", BAR = "bar" }

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test2\" }]"
            environment = { FOO = "prev-$prev{FOO}" }
            added_environment = { FOO = "prev-$prev{FOO}", BAR = "$env{BAR}" }
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![dir2_added_env.clone(), dir3_added_env.clone()]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent
            .unwrap(),
            image_container_parent!("image1", environment),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![
                dir2_added_env.clone(),
                dir4_env.clone(),
                dir4_added_env.clone()
            ]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent
            .unwrap(),
            image_container_parent!("image1", environment),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![dir2_added_env.clone(),]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .parent
            .unwrap(),
            image_container_parent!("image1", environment),
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .environment,
            vec![dir1_env.clone(), dir1_added_env.clone()],
        );
    }

    #[test]
    fn mounts() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            mounts = [ { type = "proc", mount_point = "/proc" } ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            mounts = [
                { type = "tmp", mount_point = "/tmp" },
                { type = "sys", mount_point = "/sys" },
                { type = "bind", mount_point = "/foo", local_path = "/local", read_only = true },
            ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![
                JobMount::Tmp {
                    mount_point: utf8_path_buf!("/tmp"),
                },
                JobMount::Sys {
                    mount_point: utf8_path_buf!("/sys"),
                },
                JobMount::Bind {
                    mount_point: utf8_path_buf!("/foo"),
                    local_path: utf8_path_buf!("/local"),
                    read_only: true,
                },
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Proc {
                mount_point: utf8_path_buf!("/proc")
            }],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![],
        );
    }

    #[test]
    fn added_mounts() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            added_mounts = [ { type = "tmp", mount_point = "/tmp" } ]

            [[directives]]
            filter = "package = \"package1\""
            mounts = [
                { type = "proc", mount_point = "/proc" },
            ]
            added_mounts = [
                { type = "sys", mount_point = "/sys" },
            ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            added_mounts = [
                { type = "tmp", mount_point = "/tmp" },
            ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test2\" }]"
            added_mounts = [
                { type = "tmp", mount_point = "/tmp" },
                { type = "bind", mount_point = "/foo", local_path = "/local" },
            ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test3\" }]"
            mounts = []
            added_mounts = [
                { type = "tmp", mount_point = "/tmp" },
            ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![
                JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                },
                JobMount::Sys {
                    mount_point: utf8_path_buf!("/sys"),
                },
                JobMount::Tmp {
                    mount_point: utf8_path_buf!("/tmp"),
                },
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![
                JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                },
                JobMount::Sys {
                    mount_point: utf8_path_buf!("/sys"),
                },
                JobMount::Tmp {
                    mount_point: utf8_path_buf!("/tmp"),
                },
                JobMount::Bind {
                    mount_point: utf8_path_buf!("/foo"),
                    local_path: utf8_path_buf!("/local"),
                    read_only: false,
                },
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Tmp {
                mount_point: utf8_path_buf!("/tmp")
            }],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Tmp {
                mount_point: utf8_path_buf!("/tmp")
            }],
        );
    }

    #[test]
    fn devices() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            filter = "package = \"package1\""
            mounts = [ { type = "devices", devices = [ "null" ] } ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            mounts = [ { type = "devices", devices = [ "zero", "tty" ] } ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Zero | JobDevice::Tty}
            }],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Null}
            }],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![],
        );
    }

    #[test]
    fn added_devices() {
        let all = AllMetadata::<SimpleFilter>::from_str(
            r#"
            [[directives]]
            added_mounts = [ { type = "devices", devices = [ "tty" ] } ]

            [[directives]]
            filter = "package = \"package1\""
            mounts = [ { type = "devices", devices = [ "zero", "null" ] } ]
            added_mounts = [ { type = "devices", devices = [ "full" ] } ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test1\" }]"
            added_mounts = [ { type = "devices", devices = [ "random" ] } ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test2\" }]"
            added_mounts = [ { type = "devices", devices = [ "urandom" ] } ]

            [[directives]]
            filter = "and = [{ package = \"package1\" }, { name = \"test3\" }]"
            mounts = []
            added_mounts = [ { type = "devices", devices = [ "zero" ] } ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![
                JobMount::Devices {
                    devices: enum_set! {JobDevice::Zero | JobDevice::Null}
                },
                JobMount::Devices {
                    devices: enum_set! {JobDevice::Full}
                },
                JobMount::Devices {
                    devices: enum_set! {JobDevice::Random}
                },
            ]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test2", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![
                JobMount::Devices {
                    devices: enum_set! {JobDevice::Zero | JobDevice::Null}
                },
                JobMount::Devices {
                    devices: enum_set! {JobDevice::Full}
                },
                JobMount::Devices {
                    devices: enum_set! {JobDevice::Urandom}
                },
            ]
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package1".into(),
                &"package1".into(),
                ("test3", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Zero}
            }],
        );
        assert_eq!(
            all.get_metadata_for_test(
                &"package2".into(),
                &"package2".into(),
                ("test1", &NoCaseMetadata)
            )
            .unwrap()
            .container
            .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Tty}
            }],
        );
    }

    fn assert_toml_error(err: Error, expected: &str) {
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(message.starts_with(expected), "message: {message}");
    }

    #[test]
    fn bad_field_in_all_metadata() {
        assert_toml_error(
            AllMetadata::<SimpleFilter>::from_str(
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
