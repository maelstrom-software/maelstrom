use anyhow::{Error, Result};
use cargo_metadata::{
    Artifact as CargoArtifact, Message as CargoMessage, MessageIter as CargoMessageIter,
};
use regex::Regex;
use std::{
    io,
    io::BufReader,
    path::Path,
    process::{Child, ChildStdout, Command, Stdio},
    str,
};

pub struct WaitHandle {
    child: Child,
}

impl WaitHandle {
    pub fn wait(mut self, mut stderr: impl io::Write) -> Result<()> {
        if self.child.wait()?.success() {
            Ok(())
        } else {
            std::io::copy(self.child.stderr.as_mut().unwrap(), &mut stderr)?;
            Err(Error::msg("build failure".to_string()))
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
    cargo: &str,
    color: bool,
    packages: Vec<String>,
) -> Result<(WaitHandle, TestArtifactStream)> {
    let mut cmd = Command::new(cargo);
    cmd.arg("test")
        .arg("--no-run")
        .arg("--message-format=json-render-diagnostics")
        .arg(&format!(
            "--color={}",
            if color { "always" } else { "never" }
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for package in packages {
        cmd.arg("--package").arg(package);
    }

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().unwrap();

    Ok((
        WaitHandle { child },
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
