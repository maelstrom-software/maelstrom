use crate::{
    BuildDir, PytestCaseMetadata, PytestConfigValues, PytestPackageId, PytestTestArtifact,
};
use anyhow::{anyhow, bail, Result};
use maelstrom_client::ProjectDir;
use maelstrom_util::{process::ExitCode, root::Root};
use serde::Deserialize;
use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::{fmt, io::Read as _, thread};

pub struct WaitHandle;

impl WaitHandle {
    pub fn wait(self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct PytestCollectError {
    pub stderr: String,
    pub exit_code: ExitCode,
}

impl std::error::Error for PytestCollectError {}

impl fmt::Display for PytestCollectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.stderr.fmt(f)
    }
}

pub(crate) struct TestArtifactStream(
    std::collections::hash_map::IntoValues<String, PytestTestArtifact>,
);

impl Iterator for TestArtifactStream {
    type Item = Result<PytestTestArtifact>;

    fn next(&mut self) -> Option<Result<PytestTestArtifact>> {
        self.0.next().map(Ok)
    }
}

fn compile_python(path: &Path) -> Result<()> {
    let mut cmd = Command::new("/usr/bin/env");
    cmd.args(["python", "-m", "compileall"])
        .arg(path)
        .stderr(Stdio::null())
        .stdout(Stdio::null());
    let mut child = cmd.spawn()?;

    let exit_status = child.wait()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!("failed to compile python")
    }
}

fn run_python(script: &str, cwd: &Path, args: Vec<String>) -> Result<String> {
    let mut cmd = Command::new("/usr/bin/env");
    cmd.args(["python", "-c", script])
        .args(args)
        .current_dir(cwd)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped());
    let mut child = cmd.spawn()?;

    let mut stdout = child.stdout.take().unwrap();
    let stdout_handle = thread::spawn(move || -> Result<String> {
        let mut stdout_string = String::new();
        stdout.read_to_string(&mut stdout_string)?;
        Ok(stdout_string)
    });

    let mut stderr = child.stderr.take().unwrap();
    let stderr_handle = thread::spawn(move || -> Result<String> {
        let mut stderr_string = String::new();
        stderr.read_to_string(&mut stderr_string)?;
        Ok(stderr_string)
    });

    let stdout = stdout_handle.join().unwrap()?;
    let stderr = stderr_handle.join().unwrap()?;

    let exit_status = child.wait()?;
    if exit_status.success() {
        Ok(stdout)
    } else {
        let exit_code = exit_status
            .code()
            .unwrap_or_else(|| 128 + exit_status.signal().unwrap());
        Err(PytestCollectError {
            stderr,
            exit_code: ExitCode::from(exit_code as u8),
        }
        .into())
    }
}

#[derive(Deserialize)]
struct PytestCase {
    file: String,
    name: String,
    node_id: String,
    markers: Vec<String>,
    skip: bool,
}

pub fn pytest_collect_tests(
    _color: bool,
    pytest_options: &PytestConfigValues,
    project_dir: &Root<ProjectDir>,
    build_dir: &Root<BuildDir>,
) -> Result<(WaitHandle, TestArtifactStream)> {
    compile_python(project_dir.as_ref())?;

    let mut args = vec![
        "--ignore".into(),
        build_dir
            .to_str()
            .ok_or_else(|| anyhow!("non UTF-8 path"))?
            .to_owned(),
    ];
    if let Some(arg) = &pytest_options.collect_from_module {
        args.push("--pyargs".into());
        args.push(arg.into());
    }
    let output = run_python(include_str!("py/collect_tests.py"), project_dir, args)?;
    let mut tests = HashMap::new();
    for line in output.split('\n').filter(|l| !l.is_empty()) {
        let case: PytestCase = serde_json::from_str(line)?;
        let path = Path::new(&case.file).strip_prefix(project_dir).unwrap();
        let path_str = path.to_str().unwrap().to_owned();
        let test = tests.entry(path_str.clone()).or_insert(PytestTestArtifact {
            path: path.to_path_buf(),
            tests: vec![],
            ignored_tests: vec![],
            package: PytestPackageId("default".into()),
            pytest_options: pytest_options.clone(),
        });
        if case.skip {
            test.ignored_tests.push(case.name.clone());
        }
        test.tests.push((
            case.name,
            PytestCaseMetadata {
                node_id: case.node_id,
                markers: case.markers,
            },
        ));
    }

    Ok((WaitHandle, TestArtifactStream(tests.into_values())))
}
