mod directive;

use crate::pattern;
use anyhow::{Context as _, Error, Result};
use directive::TestDirective;
use maelstrom_base::{EnumSet, GroupId, JobDevice, JobMount, Layer, UserId, Utf8PathBuf};
use maelstrom_client::spec::{self, substitute, ImageConfig, ImageOption, PossiblyImage};
use maelstrom_util::fs::Fs;
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path, str};

#[derive(PartialEq, Eq, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AllMetadata {
    directives: Vec<TestDirective>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct TestMetadata {
    include_shared_libraries: Option<bool>,
    pub enable_loopback: bool,
    pub enable_writable_file_system: bool,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
    pub layers: Vec<Layer>,
    environment: BTreeMap<String, String>,
    pub mounts: Vec<JobMount>,
    pub devices: EnumSet<JobDevice>,
}

impl Default for TestMetadata {
    fn default() -> Self {
        Self {
            include_shared_libraries: Default::default(),
            enable_loopback: Default::default(),
            enable_writable_file_system: Default::default(),
            working_directory: Utf8PathBuf::from("/"),
            user: UserId::from(0),
            group: GroupId::from(0),
            layers: Default::default(),
            environment: Default::default(),
            mounts: Default::default(),
            devices: Default::default(),
        }
    }
}

impl TestMetadata {
    /// Return whether to include a layer of shared library dependencies.
    ///
    /// The logic here is that if they explicitly set the value to something, we should return
    /// that. Otherwise, we should see if they set any layers. If they explicitly added layers,
    /// they probably don't want us pushing shared libraries on those layers.
    pub fn include_shared_libraries(&self) -> bool {
        match self.include_shared_libraries {
            Some(val) => val,
            None => self.layers.is_empty(),
        }
    }

    pub fn environment(&self) -> Vec<String> {
        self.environment
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect()
    }

    fn try_fold(
        mut self,
        directive: &TestDirective,
        env_lookup: impl Fn(&str) -> Result<Option<String>>,
        image_lookup: impl FnMut(&str) -> Result<ImageConfig>,
    ) -> Result<Self> {
        let image = ImageOption::new(&directive.image, image_lookup)?;

        if directive.include_shared_libraries.is_some() {
            self.include_shared_libraries = directive.include_shared_libraries;
        }

        if let Some(enable_loopback) = directive.enable_loopback {
            self.enable_loopback = enable_loopback;
        }

        if let Some(enable_writable_file_system) = directive.enable_writable_file_system {
            self.enable_writable_file_system = enable_writable_file_system;
        }

        match &directive.working_directory {
            Some(PossiblyImage::Explicit(working_directory)) => {
                self.working_directory = working_directory.clone();
            }
            Some(PossiblyImage::Image) => {
                self.working_directory = image.working_directory()?;
            }
            None => {}
        }

        if let Some(user) = directive.user {
            self.user = user;
        }

        if let Some(group) = directive.group {
            self.group = group;
        }

        match &directive.layers {
            Some(PossiblyImage::Explicit(layers)) => {
                self.layers = layers.to_vec();
            }
            Some(PossiblyImage::Image) => {
                self.layers = image.layers()?.collect();
            }
            None => {}
        }
        self.layers.extend(directive.added_layers.iter().cloned());

        fn substitute_environment(
            env_lookup: impl Fn(&str) -> Result<Option<String>>,
            prev: &BTreeMap<String, String>,
            new: &BTreeMap<String, String>,
        ) -> Result<Vec<(String, String)>> {
            new.iter()
                .map(|(k, v)| {
                    substitute::substitute(v, &env_lookup, |var| prev.get(var).map(String::as_str))
                        .map(|v| (k.clone(), String::from(v)))
                        .map_err(Error::new)
                })
                .collect()
        }

        match &directive.environment {
            Some(PossiblyImage::Explicit(environment)) => {
                self.environment =
                    substitute_environment(&env_lookup, &self.environment, environment)?
                        .into_iter()
                        .collect();
            }
            Some(PossiblyImage::Image) => {
                self.environment = image.environment()?;
            }
            None => {}
        }
        self.environment.extend(substitute_environment(
            &env_lookup,
            &self.environment,
            &directive.added_environment,
        )?);

        if let Some(mounts) = &directive.mounts {
            self.mounts = mounts.to_vec();
        }
        self.mounts.extend(directive.added_mounts.iter().cloned());

        if let Some(devices) = directive.devices {
            self.devices = devices;
        }
        self.devices = self.devices.union(directive.added_devices);

        Ok(self)
    }
}

fn pattern_match(filter: &pattern::Pattern, context: &pattern::Context) -> bool {
    pattern::interpret_pattern(filter, context).expect("context should have case")
}

impl AllMetadata {
    fn get_metadata_for_test(
        &self,
        context: &pattern::Context,
        env_lookup: impl Fn(&str) -> Result<Option<String>>,
        mut image_lookup: impl FnMut(&str) -> Result<ImageConfig>,
    ) -> Result<TestMetadata> {
        self.directives
            .iter()
            .filter(|directive| match directive {
                TestDirective {
                    filter: Some(filter),
                    ..
                } => pattern_match(filter, context),
                TestDirective { filter: None, .. } => true,
            })
            .try_fold(TestMetadata::default(), |m, d| {
                m.try_fold(d, &env_lookup, &mut image_lookup)
            })
    }

    pub fn get_metadata_for_test_with_env(
        &self,
        context: &pattern::Context,
        image_lookup: impl FnMut(&str) -> Result<ImageConfig>,
    ) -> Result<TestMetadata> {
        self.get_metadata_for_test(context, spec::std_env_lookup, image_lookup)
    }

    fn from_str(contents: &str) -> Result<Self> {
        Ok(toml::from_str(contents)?)
    }

    pub fn load(workspace_root: &impl AsRef<Path>) -> Result<Self> {
        let path = workspace_root.as_ref().join("maelstrom-test.toml");

        Ok(Fs::new()
            .read_to_string_if_exists(&path)?
            .map(|c| Self::from_str(&c).with_context(|| format!("parsing {}", path.display())))
            .transpose()?
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use maelstrom_base::{enum_set, JobMountFsType};
    use maelstrom_test::{path_buf_vec, string, string_vec, tar_layer, utf8_path_buf};
    use toml::de::Error as TomlError;

    fn test_ctx(package: &str, test: &str) -> pattern::Context {
        pattern::Context {
            package: package.into(),
            artifact: Some(pattern::Artifact {
                name: package.into(),
                kind: pattern::ArtifactKind::Library,
            }),
            case: Some(pattern::Case { name: test.into() }),
        }
    }

    fn empty_env(_: &str) -> Result<Option<String>> {
        Ok(None)
    }

    fn no_containers(_: &str) -> Result<ImageConfig> {
        panic!()
    }

    #[test]
    fn default() {
        assert_eq!(
            AllMetadata { directives: vec![] }
                .get_metadata_for_test(&test_ctx("mod", "foo"), empty_env, no_containers)
                .unwrap(),
            TestMetadata::default(),
        );
    }

    #[test]
    fn include_shared_libraries_defaults() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            layers = [{ tar = "layer1" }]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            layers = []
            "#,
        )
        .unwrap();
        assert!(all
            .get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
            .unwrap()
            .include_shared_libraries());
        assert!(!all
            .get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
            .unwrap()
            .include_shared_libraries());
        assert!(all
            .get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
            .unwrap()
            .include_shared_libraries());
    }

    #[test]
    fn include_shared_libraries() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            include_shared_libraries = false

            [[directives]]
            filter = "package.equals(package1)"
            include_shared_libraries = true
            layers = [{ tar = "layer1" }]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            layers = []
            "#,
        )
        .unwrap();
        assert!(all
            .get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
            .unwrap()
            .include_shared_libraries());
        assert!(all
            .get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
            .unwrap()
            .include_shared_libraries());
        assert!(!all
            .get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
            .unwrap()
            .include_shared_libraries());
    }

    #[test]
    fn enable_loopback() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            enable_loopback = true

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            enable_loopback = false
            "#,
        )
        .unwrap();
        assert!(
            !all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .enable_loopback
        );
        assert!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .enable_loopback
        );
        assert!(
            !all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .enable_loopback
        );
    }

    #[test]
    fn enable_writable_file_system() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            enable_writable_file_system = true

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            enable_writable_file_system = false
            "#,
        )
        .unwrap();
        assert!(
            !all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .enable_writable_file_system
        );
        assert!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .enable_writable_file_system
        );
        assert!(
            !all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .enable_writable_file_system
        );
    }

    #[test]
    fn working_directory() {
        let image_lookup = |name: &_| match name {
            "rust" => Ok(ImageConfig {
                working_directory: Some(utf8_path_buf!("/foo")),
                ..Default::default()
            }),
            "no-working-directory" => Ok(Default::default()),
            _ => panic!(),
        };
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            include_shared_libraries = false

            [[directives]]
            filter = "package.equals(package1)"
            image.name = "rust"
            image.use = ["working_directory"]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            working_directory = "/bar"

            [[directives]]
            filter = "package.equals(package3)"
            image.name = "no-working-directory"
            image.use = ["working_directory"]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, image_lookup)
                .unwrap()
                .working_directory,
            utf8_path_buf!("/bar")
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, image_lookup)
                .unwrap()
                .working_directory,
            utf8_path_buf!("/foo")
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, image_lookup)
                .unwrap()
                .working_directory,
            utf8_path_buf!("/")
        );
    }

    #[test]
    fn user() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            user = 101

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            user = 202
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .user,
            UserId::from(202)
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .user,
            UserId::from(101)
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .user,
            UserId::from(0)
        );
    }

    #[test]
    fn group() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            group = 101

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            group = 202
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .group,
            GroupId::from(202)
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .group,
            GroupId::from(101)
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .group,
            GroupId::from(0)
        );
    }

    #[test]
    fn layers() {
        let image_lookup = |name: &_| match name {
            "image1" => Ok(ImageConfig {
                layers: path_buf_vec!["layer11", "layer12"],
                ..Default::default()
            }),
            "image2" => Ok(ImageConfig {
                layers: path_buf_vec!["layer21", "layer22"],
                ..Default::default()
            }),
            "empty-layers" => Ok(Default::default()),
            _ => panic!(),
        };
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            layers = [{ tar = "layer1" }, { tar = "layer2" }]

            [[directives]]
            filter = "package.equals(package1)"
            image.name = "image2"
            image.use = [ "layers" ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            image.name = "image1"
            image.use = [ "layers" ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test2)"
            layers = [{ tar = "layer3" }, { tar = "layer4" }]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test3)"
            image.name = "empty-layers"
            image.use = [ "layers" ]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, image_lookup)
                .unwrap()
                .layers,
            vec![tar_layer!("layer11"), tar_layer!("layer12")],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, image_lookup)
                .unwrap()
                .layers,
            vec![tar_layer!("layer3"), tar_layer!("layer4")],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test3"), empty_env, image_lookup)
                .unwrap()
                .layers,
            vec![]
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test4"), empty_env, image_lookup)
                .unwrap()
                .layers,
            vec![tar_layer!("layer21"), tar_layer!("layer22")],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, image_lookup)
                .unwrap()
                .layers,
            vec![tar_layer!("layer1"), tar_layer!("layer2")],
        );
    }

    #[test]
    fn added_layers() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            added_layers = [{ tar = "added-layer1" }, { tar = "added-layer2" }]

            [[directives]]
            filter = "package.equals(package1)"
            layers = [{tar = "layer1" }, { tar = "layer2" }]
            added_layers = [{ tar = "added-layer3" }, { tar = "added-layer4" }]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            added_layers = [{tar = "added-layer5" }, { tar = "added-layer6" }]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
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
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
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
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .layers,
            vec![tar_layer!("added-layer1"), tar_layer!("added-layer2")],
        );
    }

    #[test]
    fn environment() {
        let env = |key: &_| {
            Ok(Some(match key {
                "FOO" => string!("env-foo"),
                "BAR" => string!("env-bar"),
                _ => panic!(),
            }))
        };
        let images = |name: &_| match name {
            "image1" => Ok(ImageConfig {
                environment: Some(vec![string!("FOO=image-foo"), string!("FROB=image-frob")]),
                ..Default::default()
            }),
            "no-environment" => Ok(Default::default()),
            "bad-environment" => Ok(ImageConfig {
                environment: Some(string_vec!["FOO"]),
                ..Default::default()
            }),
            _ => panic!(),
        };
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            environment = { FOO = "$env{FOO}", BAR = "bar", BAZ = "$prev{FOO:-no-prev-foo}" }

            [[directives]]
            filter = "package.equals(package1)"
            image.name = "image1"
            image.use = ["environment"]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            environment = { FOO = "$prev{FOO}", BAR = "$env{BAR}", BAZ = "$prev{BAZ:-no-prev-baz}" }

            [[directives]]
            filter = "package.equals(package3)"
            image.name = "no-environment"
            image.use = ["environment"]

            [[directives]]
            filter = "package.equals(package4)"
            image.name = "bad-environment"
            image.use = ["environment"]
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), env, images)
                .unwrap()
                .environment(),
            string_vec!["BAR=env-bar", "BAZ=no-prev-baz", "FOO=image-foo",],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), env, images)
                .unwrap()
                .environment(),
            string_vec!["FOO=image-foo", "FROB=image-frob"],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), env, images)
                .unwrap()
                .environment(),
            string_vec!["BAR=bar", "BAZ=no-prev-foo", "FOO=env-foo",],
        );
    }

    #[test]
    fn added_environment() {
        let env = |key: &_| {
            Ok(Some(match key {
                "FOO" => string!("env-foo"),
                "BAR" => string!("env-bar"),
                _ => panic!(),
            }))
        };
        let images = |name: &_| match name {
            "image1" => Ok(ImageConfig {
                environment: Some(string_vec!["FOO=image-foo", "FROB=image-frob",]),
                ..Default::default()
            }),
            _ => panic!(),
        };
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            environment = { FOO = "foo", BAR = "bar" }
            added_environment = { FOO = "prev-$prev{FOO}", BAZ = "$prev{BAZ:-no-prev-baz}" }

            [[directives]]
            filter = "package.equals(package1)"
            image.name = "image1"
            image.use = ["environment"]
            added_environment = { FOO = "$prev{FOO}", BAZ = "$prev{BAZ:-no-prev-baz}" }

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            added_environment = { FOO = "prev-$prev{FOO}", BAR = "bar" }

            [[directives]]
            filter = "package.equals(package1) && name.equals(test2)"
            environment = { FOO = "prev-$prev{FOO}" }
            added_environment = { FOO = "prev-$prev{FOO}", BAR = "$env{BAR}" }
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), env, images)
                .unwrap()
                .environment(),
            string_vec![
                "BAR=bar",
                "BAZ=no-prev-baz",
                "FOO=prev-image-foo",
                "FROB=image-frob",
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), env, images)
                .unwrap()
                .environment(),
            string_vec!["BAR=env-bar", "FOO=prev-prev-image-foo",],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test3"), env, images)
                .unwrap()
                .environment(),
            string_vec!["BAZ=no-prev-baz", "FOO=image-foo", "FROB=image-frob",],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), env, images)
                .unwrap()
                .environment(),
            string_vec!["BAR=bar", "BAZ=no-prev-baz", "FOO=prev-foo",],
        );
    }

    #[test]
    fn mounts() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            mounts = [ { fs_type = "proc", mount_point = "/proc" } ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            mounts = [
                { fs_type = "tmp", mount_point = "/tmp" },
                { fs_type = "sys", mount_point = "/sys" },
            ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![
                JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: utf8_path_buf!("/tmp"),
                },
                JobMount {
                    fs_type: JobMountFsType::Sys,
                    mount_point: utf8_path_buf!("/sys"),
                },
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![JobMount {
                fs_type: JobMountFsType::Proc,
                mount_point: utf8_path_buf!("/proc"),
            },],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![],
        );
    }

    #[test]
    fn added_mounts() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            added_mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]

            [[directives]]
            filter = "package.equals(package1)"
            mounts = [
                { fs_type = "proc", mount_point = "/proc" },
            ]
            added_mounts = [
                { fs_type = "sys", mount_point = "/sys" },
            ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            added_mounts = [
                { fs_type = "tmp", mount_point = "/tmp" },
            ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test2)"
            added_mounts = [
                { fs_type = "tmp", mount_point = "/tmp" },
                { fs_type = "proc", mount_point = "/proc" },
            ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test3)"
            mounts = []
            added_mounts = [
                { fs_type = "tmp", mount_point = "/tmp" },
            ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![
                JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                },
                JobMount {
                    fs_type: JobMountFsType::Sys,
                    mount_point: utf8_path_buf!("/sys"),
                },
                JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: utf8_path_buf!("/tmp"),
                },
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![
                JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                },
                JobMount {
                    fs_type: JobMountFsType::Sys,
                    mount_point: utf8_path_buf!("/sys"),
                },
                JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: utf8_path_buf!("/tmp"),
                },
                JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                },
            ],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test3"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![JobMount {
                fs_type: JobMountFsType::Tmp,
                mount_point: utf8_path_buf!("/tmp"),
            },],
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .mounts,
            vec![JobMount {
                fs_type: JobMountFsType::Tmp,
                mount_point: utf8_path_buf!("/tmp"),
            },],
        );
    }

    #[test]
    fn devices() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            filter = "package.equals(package1)"
            devices = [ "null" ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            devices = [ "zero", "tty" ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .devices,
            enum_set! {JobDevice::Zero | JobDevice::Tty},
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .devices,
            enum_set! {JobDevice::Null},
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .devices,
            EnumSet::EMPTY,
        );
    }

    #[test]
    fn added_devices() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            added_devices = [ "tty" ]

            [[directives]]
            filter = "package.equals(package1)"
            devices = [ "zero", "null" ]
            added_devices = [ "full" ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test1)"
            added_devices = [ "random" ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test2)"
            added_devices = [ "urandom" ]

            [[directives]]
            filter = "package.equals(package1) && name.equals(test3)"
            devices = []
            added_devices = [ "zero" ]
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test1"), empty_env, no_containers)
                .unwrap()
                .devices,
            enum_set! {JobDevice::Zero | JobDevice::Null | JobDevice::Full | JobDevice::Random},
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test2"), empty_env, no_containers)
                .unwrap()
                .devices,
            enum_set! {JobDevice::Zero | JobDevice::Null | JobDevice::Full | JobDevice::Urandom},
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package1", "test3"), empty_env, no_containers)
                .unwrap()
                .devices,
            enum_set! {JobDevice::Zero},
        );
        assert_eq!(
            all.get_metadata_for_test(&test_ctx("package2", "test1"), empty_env, no_containers)
                .unwrap()
                .devices,
            enum_set! {JobDevice::Tty},
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
            AllMetadata::from_str(
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
