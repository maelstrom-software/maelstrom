use anyhow::{bail, Context as _, Result};
use cargo_metadata::{
    Artifact as CargoArtifact, Message as CargoMessage, MessageIter as CargoMessageIter,
    Metadata as CargoMetadata, Package as CargoPackage, PackageId as CargoPackageId,
};
use maelstrom_macro::Config;
use maelstrom_test_runner::ui::UiSender;
use maelstrom_util::process::ExitCode;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::os::unix::process::ExitStatusExt as _;
use std::{
    ffi::OsString,
    fmt,
    io::{self, BufRead as _, BufReader},
    iter,
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
    str, thread,
};

#[derive(Debug)]
pub struct CargoBuildError {
    pub stderr: String,
    pub exit_code: ExitCode,
}

impl std::error::Error for CargoBuildError {}

impl fmt::Display for CargoBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cargo exited with {:?}\nstderr:\n{}",
            self.exit_code, self.stderr
        )
    }
}

pub struct WaitHandle {
    child: Child,
    stderr_handle: thread::JoinHandle<Result<String>>,
}

impl WaitHandle {
    pub fn wait(mut self) -> Result<()> {
        let exit_status = self.child.wait()?;
        if exit_status.success() {
            Ok(())
        } else {
            let stderr = self.stderr_handle.join().unwrap()?;
            // Do like bash does and encode the signal in the exit code
            let exit_code = exit_status
                .code()
                .unwrap_or_else(|| 128 + exit_status.signal().unwrap());
            Err(CargoBuildError {
                stderr,
                exit_code: ExitCode::from(exit_code as u8),
            }
            .into())
        }
    }
}

#[derive(Debug)]
struct PackageToBuild {
    unbuilt_non_test_binaries: HashSet<String>,
    built_tests: Vec<CargoArtifact>,
}

impl PackageToBuild {
    fn new(p: &CargoPackage) -> Self {
        Self {
            unbuilt_non_test_binaries: p
                .targets
                .iter()
                .filter(|t| t.is_bin())
                .map(|t| t.name.clone())
                .collect(),
            built_tests: vec![],
        }
    }
}

pub struct TestArtifactStream<StreamT> {
    log: slog::Logger,
    packages: HashMap<CargoPackageId, PackageToBuild>,
    ready: Vec<CargoArtifact>,
    stream: CargoMessageIter<BufReader<StreamT>>,
    build_done: bool,
}

impl<StreamT: io::Read> TestArtifactStream<StreamT> {
    fn new(
        log: slog::Logger,
        packages: HashMap<CargoPackageId, PackageToBuild>,
        stream: CargoMessageIter<BufReader<StreamT>>,
    ) -> Self {
        slog::debug!(log, "cargo test artifact stream packages"; "packages" => ?packages);

        Self {
            log,
            packages,
            ready: vec![],
            stream,
            build_done: false,
        }
    }

    fn receive_built_artifact(&mut self, artifact: CargoArtifact) -> Result<()> {
        if artifact.target.kind.iter().any(|kind| kind == "proc-macro") {
            return Ok(());
        }
        let Some(pkg) = self.packages.get_mut(&artifact.package_id) else {
            return Ok(());
        };
        if artifact.target.is_test() {
            pkg.built_tests.push(artifact);
        } else if artifact.target.is_bin() && !artifact.profile.test {
            if !pkg.unbuilt_non_test_binaries.remove(&artifact.target.name) {
                bail!(
                    "unexpected binary {} built for package {}. expected {:?}",
                    &artifact.target.name,
                    &artifact.package_id,
                    &pkg.unbuilt_non_test_binaries
                )
            }
        } else if artifact.executable.is_some() && artifact.profile.test {
            self.ready.push(artifact);
        }

        if pkg.unbuilt_non_test_binaries.is_empty() {
            self.ready.extend(std::mem::take(&mut pkg.built_tests));
        }

        Ok(())
    }

    fn build_completed(&mut self) {
        assert!(!self.build_done);
        self.build_done = true;

        for (id, pkg) in &mut self.packages {
            if !pkg.built_tests.is_empty() {
                assert!(!pkg.unbuilt_non_test_binaries.is_empty());
                for b in &pkg.unbuilt_non_test_binaries {
                    slog::warn!(
                        self.log,
                        "didn't receive build notification for {id:?} binary {b:?}"
                    );
                }
                self.ready.extend(std::mem::take(&mut pkg.built_tests));
            }
        }
    }

    fn read_next_artifact(&mut self) -> Result<()> {
        match self.stream.next() {
            Some(Err(e)) => Err(e.into()),
            Some(Ok(CargoMessage::CompilerArtifact(artifact))) => {
                self.receive_built_artifact(artifact)
            }
            Some(_) => Ok(()),
            None => {
                self.build_completed();
                Ok(())
            }
        }
    }
}

impl<StreamT: io::Read> Iterator for TestArtifactStream<StreamT> {
    type Item = Result<CargoArtifact>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(a) = self.ready.pop() {
            return Some(Ok(a));
        }

        while !self.build_done {
            if let Err(e) = self.read_next_artifact() {
                return Some(Err(e));
            }

            if let Some(a) = self.ready.pop() {
                return Some(Ok(a));
            }
        }

        None
    }
}

pub fn run_cargo_test(
    color: bool,
    feature_selection_options: &FeatureSelectionOptions,
    compilation_options: &CompilationOptions,
    manifest_options: &ManifestOptions,
    packages: Vec<&CargoPackage>,
    ui: UiSender,
    log: slog::Logger,
) -> Result<(WaitHandle, TestArtifactStream<ChildStdout>)> {
    let mut cmd = Command::new("cargo");
    cmd.arg("test")
        .arg("--no-run")
        .arg("--message-format=json-render-diagnostics")
        .arg(&format!(
            "--color={}",
            if color { "always" } else { "never" }
        ))
        .args(feature_selection_options.iter())
        .args(compilation_options.iter())
        .args(manifest_options.iter())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for p in &packages {
        cmd.arg("--package")
            .arg(format!("{}@{}", &p.name, &p.version));
    }

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().unwrap();
    let stderr = BufReader::new(child.stderr.take().unwrap());
    let stderr_handle = thread::spawn(move || {
        let mut stderr_string = String::new();
        for line in stderr.lines() {
            let line = line?;
            stderr_string += &line;
            stderr_string += "\n";
            ui.build_output_line(line);
        }
        ui.done_building();
        Ok(stderr_string)
    });

    let packages = packages
        .into_iter()
        .map(|p| (p.id.clone(), PackageToBuild::new(p)))
        .collect();

    Ok((
        WaitHandle {
            child,
            stderr_handle,
        },
        TestArtifactStream::new(
            log,
            packages,
            CargoMessage::parse_stream(BufReader::new(stdout)),
        ),
    ))
}

pub fn get_cases_from_binary(binary: &Path, filter: &Option<String>) -> Result<Vec<String>> {
    let mut cmd = Command::new(binary);
    cmd.arg("--list").arg("--format").arg("terse");
    if let Some(filter) = filter {
        cmd.arg(filter);
    }
    let output = cmd.output()?;
    Ok(Regex::new(r"\b([^ ]*): test")?
        .captures_iter(str::from_utf8(&output.stdout)?)
        .map(|capture| capture.get(1).unwrap().as_str().trim().to_string())
        .collect())
}

#[derive(Config, Debug, Default)]
pub struct FeatureSelectionOptions {
    /// Comma-separated list of features to activate.
    #[config(
        option,
        short = 'F',
        value_name = "FEATURES",
        default = r#""cargo's default""#
    )]
    pub features: Option<String>,

    /// Activate all available features.
    #[config(flag)]
    pub all_features: bool,

    /// Do not activate the `default` feature.
    #[config(flag)]
    pub no_default_features: bool,
}

impl FeatureSelectionOptions {
    pub fn iter(&self) -> impl Iterator<Item = String> {
        iter::empty()
            .chain(
                self.features
                    .as_ref()
                    .map(|features| format!("--features={features}")),
            )
            .chain(self.all_features.then_some("--all-features".into()))
            .chain(
                self.no_default_features
                    .then_some("--no-default-features".into()),
            )
    }
}

#[derive(Config, Debug, Default)]
pub struct CompilationOptions {
    /// Build artifacts with the specified profile.
    #[config(option, value_name = "PROFILE-NAME", default = r#""cargo's default""#)]
    pub profile: Option<String>,

    /// Build for the target triple.
    #[config(option, value_name = "TRIPLE", default = r#""cargo's default""#)]
    pub target: Option<String>,

    /// Directory for all generated artifacts.
    #[config(option, value_name = "DIRECTORY", default = r#""cargo's default""#)]
    pub target_dir: Option<PathBuf>,
}

impl CompilationOptions {
    pub fn iter(&self) -> impl Iterator<Item = OsString> {
        iter::empty()
            .chain(
                self.profile
                    .as_ref()
                    .map(|profile| format!("--profile={profile}").into()),
            )
            .chain(
                self.target
                    .as_ref()
                    .map(|target| format!("--target={target}").into()),
            )
            .chain(
                self.target_dir
                    .as_ref()
                    .map(|target_dir| ["--target-dir".into(), target_dir.into()])
                    .into_iter()
                    .flatten(),
            )
    }
}

#[derive(Config, Debug, Default)]
pub struct ManifestOptions {
    /// Path to Cargo.toml.
    #[config(option, value_name = "PATH", default = r#""cargo's default""#)]
    pub manifest_path: Option<PathBuf>,

    /// Require Cargo.lock and cache are up to date.
    #[config(flag)]
    pub frozen: bool,

    /// Require Cargo.lock is up to date.
    #[config(flag)]
    pub locked: bool,

    /// Run without cargo accessing the network.
    #[config(flag)]
    pub offline: bool,
}

impl ManifestOptions {
    pub fn iter(&self) -> impl Iterator<Item = OsString> {
        iter::empty()
            .chain(
                self.manifest_path
                    .as_ref()
                    .map(|manifest_path| ["--manifest-path".into(), manifest_path.into()])
                    .into_iter()
                    .flatten(),
            )
            .chain(self.frozen.then_some("--frozen".into()))
            .chain(self.locked.then_some("--locked".into()))
            .chain(self.offline.then_some("--offline".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::{hashmap, hashset};
    use serde_json::json;

    fn integration_test(package_id: &str, name: &str) -> serde_json::Value {
        let profile = json!({
            "opt_level": "3",
            "debuginfo": 0,
            "debug_assertions": false,
            "overflow_checks": false,
            "test": true
        });

        json!({
            "reason":"compiler-artifact",
            "package_id": package_id,
            "manifest_path":"/wherever",
            "target": {
                "kind": ["test"],
                "crate_types": ["bin"],
                "name": name,
                "src_path":"wherever.rs",
                "edition":"2021",
                "doc":false,
                "doctest":false,
                "test":true
            },
            "profile": profile,
            "features": [],
            "filenames": [],
            "executable": "integration_test-f00dd00d",
            "fresh": false
        })
    }

    fn package_test_binary(package_id: &str, name: &str) -> serde_json::Value {
        let profile = json!({
            "opt_level": "3",
            "debuginfo": 0,
            "debug_assertions": false,
            "overflow_checks": false,
            "test": true
        });

        json!({
            "reason": "compiler-artifact",
            "package_id": package_id,
            "manifest_path": "wherever",
            "target": {
                "kind": ["bin"],
                "crate_types": ["bin"],
                "name": name,
                "src_path": "wherever.rs",
                "edition": "2021",
                "doc": true,
                "doctest": false,
                "test": true
            },
            "profile": profile,
            "features": [],
            "filenames": [],
            "executable": "whatever-bin",
            "fresh": false
        })
    }

    fn package_binary(package_id: &str, name: &str) -> serde_json::Value {
        let profile = json!({
            "opt_level": "3",
            "debuginfo": 0,
            "debug_assertions": false,
            "overflow_checks": false,
            "test": false
        });

        json!({
            "reason": "compiler-artifact",
            "package_id": package_id,
            "manifest_path": "wherever",
            "target": {
                "kind": ["bin"],
                "crate_types": ["bin"],
                "name": name,
                "src_path": "wherever.rs",
                "edition": "2021",
                "doc": true,
                "doctest": false,
                "test": true
            },
            "profile": profile,
            "features": [],
            "filenames": [],
            "executable": "whatever-bin",
            "fresh": false
        })
    }

    fn build_test_artifact_stream(
        packages: HashMap<&str, HashSet<&str>>,
        messages: Vec<serde_json::Value>,
    ) -> TestArtifactStream<io::Cursor<Vec<u8>>> {
        let log = maelstrom_util::log::test_logger();
        let packages = packages
            .into_iter()
            .map(|(k, v)| {
                (
                    CargoPackageId { repr: k.into() },
                    PackageToBuild {
                        unbuilt_non_test_binaries: v.into_iter().map(|b| b.into()).collect(),
                        built_tests: vec![],
                    },
                )
            })
            .collect();
        let mut messages_as_bytes = vec![];
        for m in &messages {
            serde_json::to_writer(&mut messages_as_bytes, m).unwrap();
            messages_as_bytes.push(b'\n');
        }
        let inner_stream =
            CargoMessage::parse_stream(BufReader::new(io::Cursor::new(messages_as_bytes)));
        let stream = TestArtifactStream::new(log, packages, inner_stream);
        assert_eq!(&stream.ready, &[]);
        stream
    }

    fn assert_num_reads(
        stream: &mut TestArtifactStream<io::Cursor<Vec<u8>>>,
        num_reads: usize,
        expected_ready: usize,
    ) {
        for _ in 0..num_reads {
            stream.read_next_artifact().unwrap();
            assert_eq!(stream.ready.len(), expected_ready, "{:?}", &stream.ready);
            assert!(!stream.build_done);
        }
    }

    #[test]
    fn test_artifact_stream_integration_test_before_required_binary() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin" } },
            vec![
                integration_test("foo", "int"),
                package_binary("foo", "foo-bin"),
            ],
        );
        assert_num_reads(&mut stream, 1 /* reads */, 0 /* num_ready */);
        assert_num_reads(&mut stream, 1 /* reads */, 1 /* num_ready */); // int

        stream.read_next_artifact().unwrap();
        assert!(stream.build_done);
    }

    #[test]
    fn test_artifact_stream_integration_test_after_required_binary() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin" } },
            vec![
                package_binary("foo", "foo-bin"),
                integration_test("foo", "int"),
            ],
        );
        assert_num_reads(&mut stream, 1 /* reads */, 0 /* num_ready */);
        assert_num_reads(&mut stream, 1 /* reads */, 1 /* num_ready */); // int

        stream.read_next_artifact().unwrap();
        assert!(stream.build_done);
    }

    #[test]
    fn test_artifact_stream_integration_test_between_required_binaries() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin", "foo-bin2" } },
            vec![
                package_binary("foo", "foo-bin2"),
                integration_test("foo", "int"),
                package_binary("foo", "foo-bin"),
            ],
        );
        assert_num_reads(&mut stream, 2 /* reads */, 0 /* num_ready */);
        assert_num_reads(&mut stream, 1 /* reads */, 1 /* num_ready */); // int

        stream.read_next_artifact().unwrap();
        assert!(stream.build_done);
    }

    #[test]
    fn test_artifact_stream_multiple_integration_tests_between_required_binaries() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin", "foo-bin2" } },
            vec![
                package_binary("foo", "foo-bin2"),
                integration_test("foo", "int1"),
                package_binary("foo", "foo-bin"),
                integration_test("foo", "int2"),
            ],
        );
        assert_num_reads(&mut stream, 2 /* reads */, 0 /* num_ready */);
        assert_num_reads(&mut stream, 1 /* reads */, 1 /* num_ready */); // int1
        assert_num_reads(&mut stream, 1 /* reads */, 2 /* num_ready */); // int1, int2

        stream.read_next_artifact().unwrap();
        assert!(stream.build_done);
    }

    #[test]
    fn test_artifact_stream_test_binaries_immediately_forwarded() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin", "foo-bin2" } },
            vec![
                package_test_binary("foo", "test1"),
                integration_test("foo", "int1"),
                package_test_binary("foo", "test2"),
                package_binary("foo", "foo-bin2"),
                package_binary("foo", "foo-bin"),
            ],
        );
        assert_num_reads(&mut stream, 2 /* reads */, 1 /* num_ready */); // test1
        assert_num_reads(&mut stream, 2 /* reads */, 2 /* num_ready */); // test1, test2
        assert_num_reads(&mut stream, 1 /* reads */, 3 /* num_ready */); // test1, test2, int1

        stream.read_next_artifact().unwrap();
        assert!(stream.build_done);
    }

    #[test]
    fn test_artifact_stream_missing_binary() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin" } },
            vec![
                integration_test("foo", "int1"),
                integration_test("foo", "int2"),
            ],
        );
        assert_num_reads(&mut stream, 2 /* reads */, 0 /* num_ready */);

        // we get them when the build finishes
        stream.read_next_artifact().unwrap();
        assert!(stream.build_done);
        assert_eq!(stream.ready.len(), 2, "{:?}", &stream.ready);
    }

    #[test]
    fn test_artifact_stream_unknown_binary_errors() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin" } },
            vec![package_binary("foo", "foo-bin-unknown")],
        );
        stream.read_next_artifact().unwrap_err();
    }

    #[test]
    fn test_artifact_stream_unknown_package_ignored() {
        let mut stream = build_test_artifact_stream(
            hashmap! { "foo" => hashset! { "foo-bin" } },
            vec![
                package_binary("bar", "foo-bin"),
                integration_test("foo", "int"),
            ],
        );

        assert_num_reads(&mut stream, 2 /* reads */, 0 /* num_ready */);
    }

    #[test]
    fn feature_selection_options_iter_default() {
        assert_eq!(
            Vec::<String>::from_iter(FeatureSelectionOptions::default().iter()),
            Vec::<String>::new(),
        );
    }

    #[test]
    fn feature_selection_options_iter_features() {
        let options = FeatureSelectionOptions {
            features: Some("feature1,feature2".into()),
            ..Default::default()
        };
        assert_eq!(
            Vec::<String>::from_iter(options.iter()),
            Vec::<String>::from_iter(["--features=feature1,feature2".into()]),
        );
    }

    #[test]
    fn feature_selection_options_iter_all_features() {
        let options = FeatureSelectionOptions {
            all_features: true,
            ..Default::default()
        };
        assert_eq!(
            Vec::<String>::from_iter(options.iter()),
            Vec::<String>::from_iter(["--all-features".into()]),
        );
    }

    #[test]
    fn feature_selection_options_iter_no_default_features() {
        let options = FeatureSelectionOptions {
            no_default_features: true,
            ..Default::default()
        };
        assert_eq!(
            Vec::<String>::from_iter(options.iter()),
            Vec::<String>::from_iter(["--no-default-features".into()]),
        );
    }

    #[test]
    fn feature_selection_options_iter_all() {
        let options = FeatureSelectionOptions {
            features: Some("feature1,feature2".into()),
            all_features: true,
            no_default_features: true,
        };
        assert_eq!(
            Vec::<String>::from_iter(options.iter()),
            Vec::<String>::from_iter([
                "--features=feature1,feature2".into(),
                "--all-features".into(),
                "--no-default-features".into(),
            ]),
        );
    }

    #[test]
    fn compilation_options_iter_default() {
        assert_eq!(
            Vec::<OsString>::from_iter(CompilationOptions::default().iter()),
            Vec::<OsString>::new(),
        );
    }

    #[test]
    fn compilation_options_iter_profile() {
        let options = CompilationOptions {
            profile: Some("a-profile".into()),
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--profile=a-profile".into()]),
        );
    }

    #[test]
    fn compilation_options_iter_target() {
        let options = CompilationOptions {
            target: Some("a-target".into()),
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--target=a-target".into()]),
        );
    }

    #[test]
    fn compilation_options_iter_target_dir() {
        let options = CompilationOptions {
            target_dir: Some("a-target-dir".into()),
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--target-dir".into(), "a-target-dir".into()]),
        );
    }

    #[test]
    fn compilation_options_iter_all() {
        let options = CompilationOptions {
            profile: Some("profile".into()),
            target: Some("target".into()),
            target_dir: Some("target_dir".into()),
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter([
                "--profile=profile".into(),
                "--target=target".into(),
                "--target-dir".into(),
                "target_dir".into(),
            ]),
        );
    }

    #[test]
    fn manifest_options_iter_default() {
        assert_eq!(
            Vec::<OsString>::from_iter(ManifestOptions::default().iter()),
            Vec::<OsString>::new(),
        );
    }

    #[test]
    fn manifest_options_iter_manifest_path() {
        let options = ManifestOptions {
            manifest_path: Some("manifest_path".into()),
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--manifest-path".into(), "manifest_path".into()]),
        );
    }

    #[test]
    fn manifest_options_iter_frozen() {
        let options = ManifestOptions {
            frozen: true,
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--frozen".into()]),
        );
    }

    #[test]
    fn manifest_options_iter_locked() {
        let options = ManifestOptions {
            locked: true,
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--locked".into()]),
        );
    }

    #[test]
    fn manifest_options_iter_offline() {
        let options = ManifestOptions {
            offline: true,
            ..Default::default()
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter(["--offline".into()]),
        );
    }

    #[test]
    fn manifest_options_iter_all() {
        let options = ManifestOptions {
            manifest_path: Some("manifest_path".into()),
            frozen: true,
            locked: true,
            offline: true,
        };
        assert_eq!(
            Vec::<OsString>::from_iter(options.iter()),
            Vec::<OsString>::from_iter([
                "--manifest-path".into(),
                "manifest_path".into(),
                "--frozen".into(),
                "--locked".into(),
                "--offline".into(),
            ]),
        );
    }
}

pub fn read_metadata(
    cargo_feature_selection_options: &FeatureSelectionOptions,
    cargo_manifest_options: &ManifestOptions,
) -> Result<CargoMetadata> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .args(cargo_feature_selection_options.iter())
        .args(cargo_manifest_options.iter())
        .output()
        .context("getting cargo metadata")?;
    if !output.status.success() {
        bail!(String::from_utf8(output.stderr)
            .context("reading stderr")?
            .trim_end()
            .trim_start_matches("error: ")
            .to_owned());
    }
    let cargo_metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata")?;
    Ok(cargo_metadata)
}
