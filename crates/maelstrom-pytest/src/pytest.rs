use crate::PytestTestArtifact;
use anyhow::Result;
use std::fmt;

pub struct WaitHandle;

impl WaitHandle {
    pub fn wait(self) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct PytestCollectError;

impl fmt::Display for PytestCollectError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

pub(crate) struct TestArtifactStream;

impl Iterator for TestArtifactStream {
    type Item = Result<PytestTestArtifact>;

    fn next(&mut self) -> Option<Result<PytestTestArtifact>> {
        unimplemented!()
    }
}

pub fn pytest_collect_tests(
    _color: bool,
    _packages: Vec<String>,
) -> Result<(WaitHandle, TestArtifactStream)> {
    unimplemented!()
}
