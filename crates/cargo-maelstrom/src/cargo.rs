use anyhow::Result;
use cargo_metadata::{
    Artifact as CargoArtifact, Message as CargoMessage, MessageIter as CargoMessageIter,
};
use maelstrom_macro::Config;
use maelstrom_util::process::ExitCode;
use regex::Regex;
use std::os::unix::process::ExitStatusExt as _;
use std::{
    ffi::OsString,
    fmt,
    io::{BufReader, Read as _},
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

pub struct TestArtifactStream {
    stream: CargoMessageIter<BufReader<ChildStdout>>,
}

impl Iterator for TestArtifactStream {
    type Item = Result<CargoArtifact>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.stream.next()? {
                Err(e) => return Some(Err(e.into())),
                Ok(CargoMessage::CompilerArtifact(artifact)) => {
                    if artifact.target.kind.iter().any(|kind| kind == "proc-macro") {
                        continue;
                    }
                    if artifact.executable.is_some() && artifact.profile.test {
                        return Some(Ok(artifact));
                    }
                }
                _ => continue,
            }
        }
    }
}

pub fn run_cargo_test(
    color: bool,
    feature_selection_options: &FeatureSelectionOptions,
    compilation_options: &CompilationOptions,
    manifest_options: &ManifestOptions,
    packages: Vec<String>,
) -> Result<(WaitHandle, TestArtifactStream)> {
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

    for package in packages {
        cmd.arg("--package").arg(package);
    }

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stderr_handle = thread::spawn(move || {
        let mut stderr_string = String::new();
        stderr.read_to_string(&mut stderr_string)?;
        Ok(stderr_string)
    });

    Ok((
        WaitHandle {
            child,
            stderr_handle,
        },
        TestArtifactStream {
            stream: CargoMessage::parse_stream(BufReader::new(stdout)),
        },
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
    /// Comma separated list of features to activate.
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
