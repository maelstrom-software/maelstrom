mod directive;

use crate::TestFilter;
use anyhow::{anyhow, Context as _, Result};
use directive::TestDirective;
use enumset::enum_set;
use maelstrom_base::{
    GroupId, JobDeviceForTomlAndJson, JobMount, JobMountForTomlAndJson, JobNetwork, Timeout,
    UserId, Utf8PathBuf,
};
use maelstrom_client::{
    spec::{EnvironmentSpec, ImageSpec, Layer, PossiblyImage},
    ProjectDir,
};
use maelstrom_util::{fs::Fs, root::Root, template::TemplateVars};
use serde::Deserialize;
use std::{
    fmt::Display,
    str::{self, FromStr},
};

/// This file is what we write out for the user when `--init` is provided. It should contain the
/// same data as `AllMetadata::default()` but it contains nice formatting, comments, and examples.
pub const DEFAULT_TEST_METADATA: &str = include_str!("default-test-metadata.toml");

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AllMetadata<TestFilterT> {
    #[serde(bound(deserialize = "TestFilterT: FromStr, TestFilterT::Err: Display"))]
    directives: Vec<TestDirective<TestFilterT>>,
}

impl<TestFilterT> Default for AllMetadata<TestFilterT> {
    /// The default test metadata is used when there is no configuration file. It provides some
    /// commonly used functionality by test processes in an attempt to give a good default.
    fn default() -> Self {
        let single_directive = TestDirective {
            filter: None,
            include_shared_libraries: None,
            network: None,
            enable_writable_file_system: None,
            working_directory: None,
            user: None,
            group: None,
            timeout: None,
            // Create directories and files for mounting special file-systems and device files
            layers: Some(PossiblyImage::Explicit(vec![Layer::Stubs {
                stubs: vec![
                    "/{proc,sys,tmp}/".into(),
                    "/dev/{full,null,random,urandom,zero}".into(),
                ],
            }])),
            environment: None,
            mounts: Some(vec![
                // Mount a tempfs at /tmp. Many tests use this for creating temporary files.
                JobMountForTomlAndJson::Tmp {
                    mount_point: "/tmp".into(),
                },
                // Mount proc at /proc. It is somewhat common to access information about the
                // current process via files in /proc/self
                JobMountForTomlAndJson::Proc {
                    mount_point: "/proc".into(),
                },
                // Mount sys at /sys. Accessing OS configuration values in /sys/kernel can be
                // somewhat common.
                JobMountForTomlAndJson::Sys {
                    mount_point: "/sys".into(),
                },
                // These special devices are fairly commonly used. Especially /dev/null.
                JobMountForTomlAndJson::Devices {
                    devices: enum_set!(
                        JobDeviceForTomlAndJson::Full
                            | JobDeviceForTomlAndJson::Null
                            | JobDeviceForTomlAndJson::Random
                            | JobDeviceForTomlAndJson::Urandom
                            | JobDeviceForTomlAndJson::Zero
                    ),
                },
            ]),
            added_environment: Default::default(),
            added_layers: vec![],
            added_mounts: vec![],
            image: None,
        };
        Self {
            directives: vec![single_directive],
        }
    }
}

#[test]
fn all_metadata_default_matches_default_file() {
    // Verifies that it actually parses as valid TOML
    let parsed_default_file =
        AllMetadata::<crate::SimpleFilter>::from_str(DEFAULT_TEST_METADATA).unwrap();

    // Matches what do when there is no file.
    assert_eq!(parsed_default_file, AllMetadata::default());
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TestMetadata {
    include_shared_libraries: Option<bool>,
    pub image: Option<ImageSpec>,
    pub network: JobNetwork,
    pub enable_writable_file_system: bool,
    pub working_directory: Option<Utf8PathBuf>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
    pub timeout: Option<Timeout>,
    pub layers: Vec<Layer>,
    pub environment: Vec<EnvironmentSpec>,
    pub mounts: Vec<JobMount>,
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
            None => self.image.is_none(),
        }
    }

    fn try_fold<TestFilterT>(
        mut self,
        &TestDirective {
            filter: _,
            ref image,
            include_shared_libraries,
            network,
            enable_writable_file_system,
            user,
            group,
            timeout,
            ref layers,
            ref added_layers,
            ref mounts,
            ref added_mounts,
            ref environment,
            ref added_environment,
            ref working_directory,
        }: &TestDirective<TestFilterT>,
    ) -> Result<Self> {
        let mut image = image.as_ref().map(|image| ImageSpec {
            name: image.into(),
            use_environment: false,
            use_layers: false,
            use_working_directory: false,
        });

        self.include_shared_libraries = include_shared_libraries.or(self.include_shared_libraries);
        self.network = network.unwrap_or(self.network);
        self.enable_writable_file_system =
            enable_writable_file_system.unwrap_or(self.enable_writable_file_system);
        self.user = user.or(self.user);
        self.group = group.or(self.group);
        self.timeout = timeout.unwrap_or(self.timeout);

        match layers {
            Some(PossiblyImage::Explicit(layers)) => {
                self.layers = layers.to_vec();
            }
            Some(PossiblyImage::Image) => {
                let image = image.as_mut().ok_or_else(|| anyhow!("no image provided"))?;
                image.use_layers = true;
                self.layers = vec![];
            }
            None => {}
        }
        self.layers.extend(added_layers.iter().cloned());

        self.mounts = mounts.as_ref().map_or(self.mounts, |mounts| {
            mounts.iter().cloned().map(Into::into).collect()
        });
        self.mounts
            .extend(added_mounts.iter().cloned().map(Into::into));

        match environment {
            Some(PossiblyImage::Explicit(environment)) => {
                self.environment.push(EnvironmentSpec {
                    vars: environment.clone(),
                    extend: false,
                });
            }
            Some(PossiblyImage::Image) => {
                let image = image.as_mut().ok_or_else(|| anyhow!("no image provided"))?;
                image.use_environment = true;
            }
            None => {}
        }
        if !added_environment.is_empty() {
            self.environment.push(EnvironmentSpec {
                vars: added_environment.clone(),
                extend: true,
            });
        }

        match working_directory {
            Some(PossiblyImage::Explicit(working_directory)) => {
                self.working_directory = Some(working_directory.clone());
            }
            Some(PossiblyImage::Image) => {
                let image = image.as_mut().ok_or_else(|| anyhow!("no image provided"))?;
                image.use_working_directory = true;
                self.working_directory = None;
            }
            None => {}
        }

        if image.is_some() {
            self.image = image;
        }

        Ok(self)
    }
}

impl<TestFilterT: TestFilter> AllMetadata<TestFilterT>
where
    TestFilterT::Err: Display,
{
    pub fn replace_template_vars(&mut self, vars: &TemplateVars) -> Result<()> {
        for directive in &mut self.directives {
            if let Some(PossiblyImage::Explicit(layers)) = &mut directive.layers {
                for layer in layers {
                    layer.replace_template_vars(vars)?;
                }
            }
            for added_layer in &mut directive.added_layers {
                added_layer.replace_template_vars(vars)?;
            }
        }
        Ok(())
    }

    fn get_metadata_for_test(
        &self,
        package: &str,
        artifact: &TestFilterT::ArtifactKey,
        case: (&str, &TestFilterT::CaseMetadata),
    ) -> Result<TestMetadata> {
        self.directives
            .iter()
            .filter(|directive| match directive {
                TestDirective {
                    filter: Some(filter),
                    ..
                } => filter
                    .filter(package, Some(artifact), Some(case))
                    .expect("should have case"),
                TestDirective { filter: None, .. } => true,
            })
            .try_fold(TestMetadata::default(), |m, d| m.try_fold(d))
    }

    pub fn get_metadata_for_test_with_env(
        &self,
        package: &str,
        artifact: &TestFilterT::ArtifactKey,
        case: (&str, &TestFilterT::CaseMetadata),
    ) -> Result<TestMetadata> {
        self.get_metadata_for_test(package, artifact, case)
    }

    fn from_str(contents: &str) -> Result<Self> {
        Ok(toml::from_str(contents)?)
    }

    pub fn load(
        log: slog::Logger,
        project_dir: impl AsRef<Root<ProjectDir>>,
        maelstrom_test_toml: &str,
    ) -> Result<Self> {
        struct MaelstromTestTomlFile;
        let path1 = project_dir
            .as_ref()
            .join::<MaelstromTestTomlFile>(maelstrom_test_toml);
        if let Some(contents) = Fs::new().read_to_string_if_exists(&path1)? {
            return Self::from_str(&contents)
                .with_context(|| format!("parsing {}", path1.display()));
        }

        let path2 = project_dir
            .as_ref()
            .join::<MaelstromTestTomlFile>("maelstrom-test.toml");
        if let Some(contents) = Fs::new().read_to_string_if_exists(&path2)? {
            slog::warn!(
                log,
                "use of `maelstrom-test.toml` file is deprecated and support will \
                be removed in future releases. Please rename it to `{maelstrom_test_toml}`."
            );
            return Self::from_str(&contents)
                .with_context(|| format!("parsing {}", path2.display()));
        }

        slog::debug!(
            log,
            "no test metadata configuration found, using default";
            "search_paths" => ?[path1, path2]
        );
        Ok(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoCaseMetadata, SimpleFilter};
    use anyhow::Error;
    use maelstrom_base::{enum_set, JobDevice};
    use maelstrom_test::{tar_layer, utf8_path_buf};
    use maelstrom_util::root::RootBuf;
    use maplit::btreemap;
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
                .into_iter()
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
        let res = AllMetadata::<SimpleFilter>::load(log, &project_dir, "simple-test.toml").unwrap();

        let log_lines = log_output.lines();

        (res, log_lines)
    }

    #[test]
    fn load_no_file_found() {
        let t = tempfile::tempdir().unwrap();
        let (res, log_lines) = load_test(&t);

        assert_eq!(res, AllMetadata::default());

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
                directives: vec![TestDirective::default()]
            }
        );
        assert_eq!(log_lines.len(), 0, "{log_lines:?}");
    }

    #[test]
    fn load_deprecated_file() {
        let fs = Fs::new();
        let t = tempfile::tempdir().unwrap();
        fs.write(t.path().join("maelstrom-test.toml"), "[[directives]]")
            .unwrap();

        let (res, log_lines) = load_test(&t);

        assert_eq!(
            res,
            AllMetadata {
                directives: vec![TestDirective::default()]
            }
        );
        assert_eq!(log_lines.len(), 1, "{log_lines:?}");
        assert_eq!(
            log_lines[0]["msg"],
            "use of `maelstrom-test.toml` file is deprecated and support will be removed in \
            future releases. Please rename it to `simple-test.toml`."
        );
        assert_eq!(log_lines[0]["level"], "WARN");
    }

    #[test]
    fn default() {
        assert_eq!(
            AllMetadata::<SimpleFilter> { directives: vec![] }
                .get_metadata_for_test("mod", &"mod".into(), ("foo", &NoCaseMetadata))
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
            .get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
            .unwrap()
            .include_shared_libraries());
        assert!(!all
            .get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
            .unwrap()
            .include_shared_libraries());
        assert!(all
            .get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
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
            .get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
            .unwrap()
            .include_shared_libraries());
        assert!(all
            .get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
            .unwrap()
            .include_shared_libraries());
        assert!(!all
            .get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .network,
            JobNetwork::Loopback,
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .network,
            JobNetwork::Local,
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .network,
            JobNetwork::Disabled,
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .network,
            JobNetwork::Disabled,
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
        assert!(
            !all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .enable_writable_file_system
        );
        assert!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .enable_writable_file_system
        );
        assert!(
            !all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .enable_writable_file_system
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .working_directory,
            Some(utf8_path_buf!("/bar"))
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .image,
            Some(ImageSpec {
                name: "rust".into(),
                use_environment: false,
                use_layers: false,
                use_working_directory: true,
            })
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .working_directory,
            None,
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .user,
            Some(UserId::from(202)),
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .user,
            Some(UserId::from(101)),
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .group,
            Some(GroupId::from(202)),
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .group,
            Some(GroupId::from(101)),
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .timeout,
            None,
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .timeout,
            Timeout::new(100),
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .layers,
            vec![]
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .image,
            Some(ImageSpec {
                name: "image1".into(),
                use_environment: false,
                use_layers: true,
                use_working_directory: false,
            })
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .layers,
            vec![tar_layer!("layer3"), tar_layer!("layer4")],
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .image,
            Some(ImageSpec {
                name: "image2".into(),
                use_environment: false,
                use_layers: true,
                use_working_directory: false,
            })
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .layers,
            vec![],
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .image,
            Some(ImageSpec {
                name: "image3".into(),
                use_environment: false,
                use_layers: true,
                use_working_directory: false,
            })
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test4", &NoCaseMetadata))
                .unwrap()
                .layers,
            vec![],
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test4", &NoCaseMetadata))
                .unwrap()
                .image,
            Some(ImageSpec {
                name: "image2".into(),
                use_environment: false,
                use_layers: true,
                use_working_directory: false,
            })
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .layers,
            vec![tar_layer!("layer1"), tar_layer!("layer2")],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .image,
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .layers,
            vec![
                tar_layer!("layer1"),
                tar_layer!("layer2"),
                tar_layer!("added-layer3"),
                tar_layer!("added-layer4")
            ],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .environment,
            vec![dir1_env.clone(), dir3_env.clone()]
        );
        assert!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .image
                .unwrap()
                .use_environment,
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .environment,
            vec![dir1_env.clone()]
        );
        assert!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .image
                .unwrap()
                .use_environment,
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .environment,
            vec![
                dir1_env.clone(),
                dir1_added_env.clone(),
                dir2_added_env.clone(),
                dir3_added_env.clone()
            ]
        );
        assert!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .image
                .unwrap()
                .use_environment,
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .environment,
            vec![
                dir1_env.clone(),
                dir1_added_env.clone(),
                dir2_added_env.clone(),
                dir4_env.clone(),
                dir4_added_env.clone()
            ]
        );
        assert!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .image
                .unwrap()
                .use_environment,
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .environment,
            vec![
                dir1_env.clone(),
                dir1_added_env.clone(),
                dir2_added_env.clone(),
            ]
        );
        assert!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .image
                .unwrap()
                .use_environment,
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .environment,
            vec![dir1_env.clone(), dir1_added_env.clone(),]
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .mounts,
            vec![JobMount::Proc {
                mount_point: utf8_path_buf!("/proc")
            }],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .mounts,
            vec![JobMount::Tmp {
                mount_point: utf8_path_buf!("/tmp")
            }],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
                .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Zero | JobDevice::Tty}
            }],
        );
        assert_eq!(
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
                .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Null}
            }],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test2", &NoCaseMetadata))
                .unwrap()
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
            all.get_metadata_for_test("package1", &"package1".into(), ("test3", &NoCaseMetadata))
                .unwrap()
                .mounts,
            vec![JobMount::Devices {
                devices: enum_set! {JobDevice::Zero}
            }],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", &"package2".into(), ("test1", &NoCaseMetadata))
                .unwrap()
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
