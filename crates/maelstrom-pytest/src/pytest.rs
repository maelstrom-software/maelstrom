use crate::{PytestPackageId, PytestTestArtifact};
use anyhow::Result;
use std::collections::HashMap;
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
    stderr: String,
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

fn run_python(script: &str) -> Result<String> {
    let mut cmd = Command::new("/usr/bin/env");
    cmd.args(["python", "-c", script])
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
        Err(PytestCollectError { stderr }.into())
    }
}

pub fn pytest_collect_tests(
    _color: bool,
    _packages: Vec<String>,
) -> Result<(WaitHandle, TestArtifactStream)> {
    let output = run_python(include_str!("py/collect_tests.py"))?;
    let mut tests = HashMap::new();
    for line in output.split('\n').filter(|l| !l.is_empty()) {
        let (file, case) = line.split_once("::").unwrap();
        let test = tests.entry(file.to_owned()).or_insert(PytestTestArtifact {
            name: file.into(),
            path: file.into(),
            tests: vec![],
            ignored_tests: vec![],
            package: PytestPackageId("default".into()),
        });
        test.tests.push(case.into());
    }

    Ok((WaitHandle, TestArtifactStream(tests.into_values())))
}
