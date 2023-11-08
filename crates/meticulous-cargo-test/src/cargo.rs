use anyhow::{Error, Result};
use cargo_metadata::{
    Artifact as CargoArtifact, Message as CargoMessage, MessageIter as CargoMessageIter,
};
use regex::Regex;
use std::io;
use std::{
    io::BufReader,
    process::{Child, ChildStdout, Command, Stdio},
    str,
};

pub struct CargoBuild {
    child: Child,
}

impl CargoBuild {
    pub fn new(program: &str, color: bool, package: Option<String>) -> Result<Self> {
        let mut cmd = Command::new(program);
        cmd.arg("test")
            .arg("--no-run")
            .arg("--message-format=json-render-diagnostics")
            .arg(&format!(
                "--color={}",
                if color { "always" } else { "never" }
            ))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(package) = package {
            cmd.arg("--package").arg(package);
        }

        let child = cmd.spawn()?;

        Ok(Self { child })
    }

    pub fn artifact_stream(&mut self) -> TestArtifactStream {
        TestArtifactStream {
            stream: CargoMessage::parse_stream(BufReader::new(self.child.stdout.take().unwrap())),
        }
    }

    pub fn check_status(mut self, mut stderr: impl io::Write) -> Result<()> {
        let exit_status = self.child.wait()?;
        if !exit_status.success() {
            std::io::copy(self.child.stderr.as_mut().unwrap(), &mut stderr)?;
            return Err(Error::msg("build failure".to_string()));
        }

        Ok(())
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

pub fn get_cases_from_binary(binary: &str, filter: &Option<String>) -> Result<Vec<String>> {
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
