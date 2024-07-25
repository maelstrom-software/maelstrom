use crate::{GoModuleImportPath, GoTestArtifact};
use anyhow::{Context as _, Result};
use maelstrom_test_runner::ui::UiSender;
use maelstrom_util::process::ExitCode;
use serde::Deserialize;
use std::os::unix::process::ExitStatusExt as _;
use std::{
    fmt,
    io::{BufRead as _, BufReader},
    path::Path,
    path::PathBuf,
    process::{Command, Stdio},
    str,
    sync::mpsc,
    thread,
};

#[derive(Debug)]
pub struct BuildError {
    pub stderr: String,
    pub exit_code: ExitCode,
}

impl std::error::Error for BuildError {}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "go test exited with {:?}\nstderr:\n{}",
            self.exit_code, self.stderr
        )
    }
}

pub struct WaitHandle {
    handle: thread::JoinHandle<Result<()>>,
}

impl WaitHandle {
    pub fn wait(self) -> Result<()> {
        self.handle.join().unwrap()
    }
}

pub(crate) struct TestArtifactStream {
    recv: mpsc::Receiver<GoTestArtifact>,
}

impl Iterator for TestArtifactStream {
    type Item = Result<GoTestArtifact>;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv.recv().ok().map(Ok)
    }
}

fn go_build(dir: &Path, ui: UiSender) -> Result<String> {
    let mut child = Command::new("go")
        .current_dir(dir)
        .arg("test")
        .arg("-c")
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let ui_clone = ui.clone();
    let stdout_handle = thread::spawn(move || -> Result<String> {
        let mut stdout_string = String::new();
        for line in stdout.lines() {
            let line = line?;
            stdout_string += &line;
            stdout_string += "\n";
            ui_clone.build_output_line(line);
        }
        Ok(stdout_string)
    });

    let stderr = BufReader::new(child.stderr.take().unwrap());
    let ui_clone = ui.clone();
    let stderr_handle = thread::spawn(move || -> Result<String> {
        let mut stderr_string = String::new();
        for line in stderr.lines() {
            let line = line?;
            stderr_string += &line;
            stderr_string += "\n";
            ui_clone.build_output_line(line);
        }
        Ok(stderr_string)
    });

    let stdout = stdout_handle.join().unwrap()?;
    let stderr = stderr_handle.join().unwrap()?;

    let exit_status = child.wait()?;
    if exit_status.success() {
        Ok(stdout)
    } else {
        // Do like bash does and encode the signal in the exit code
        let exit_code = exit_status
            .code()
            .unwrap_or_else(|| 128 + exit_status.signal().unwrap());
        Err(BuildError {
            stderr,
            exit_code: ExitCode::from(exit_code as u8),
        }
        .into())
    }
}

fn is_no_go_files_error<V>(res: &Result<V>) -> bool {
    if let Err(e) = res {
        if let Some(e) = e.downcast_ref::<BuildError>() {
            return e.stderr.contains("no Go files");
        }
    }
    false
}

fn multi_go_build(
    modules: Vec<GoModule>,
    send: mpsc::Sender<GoTestArtifact>,
    ui: UiSender,
) -> Result<()> {
    let mut handles = vec![];
    for m in modules {
        let send_clone = send.clone();
        let ui_clone = ui.clone();
        handles.push(thread::spawn(move || -> Result<()> {
            let res = go_build(&m.dir, ui_clone);
            if is_no_go_files_error(&res) {
                return Ok(());
            }

            let output = res?;
            if !output.contains("[no test files]") {
                let _ = send_clone.send(GoTestArtifact {
                    id: GoModuleImportPath(m.import_path.clone()),
                    name: m.name.clone(),
                    path: m.dir.join(format!("{}.test", &m.name)),
                });
            }
            Ok(())
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    ui.done_building();

    for res in results {
        res?
    }

    Ok(())
}

pub(crate) fn build_and_collect(
    _color: bool,
    modules: Vec<&GoModule>,
    ui: UiSender,
) -> Result<(WaitHandle, TestArtifactStream)> {
    let paths = modules.into_iter().cloned().collect();
    let (send, recv) = mpsc::channel();
    let handle = thread::spawn(move || multi_go_build(paths, send, ui));
    Ok((WaitHandle { handle }, TestArtifactStream { recv }))
}

pub fn get_cases_from_binary(binary: &Path, filter: &Option<String>) -> Result<Vec<String>> {
    let filter = filter.as_ref().map(|s| s.as_str()).unwrap_or(".");

    let output = Command::new(binary)
        .arg(format!("-test.list={filter}"))
        .output()
        .with_context(|| format!("running binary {}", binary.display()))?;
    Ok(str::from_utf8(&output.stdout)?
        .split('\n')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_owned())
        .collect())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoModule {
    pub dir: PathBuf,
    pub import_path: String,
    pub name: String,
    #[allow(dead_code)]
    pub root: PathBuf,
}

fn go_list(dir: &Path) -> Result<Vec<GoModule>> {
    let output = Command::new("go")
        .current_dir(dir)
        .arg("list")
        .arg("-json")
        .arg("./...")
        .output()?;
    let mut modules = vec![];
    let mut cursor = &output.stdout[..];
    while !cursor.is_empty() {
        let mut d = serde_json::Deserializer::new(serde_json::de::IoRead::new(&mut cursor));
        let m: GoModule = serde::Deserialize::deserialize(&mut d)?;
        modules.push(m);
        while !cursor.is_empty() && (cursor[0] as char).is_ascii_whitespace() {
            cursor = &cursor[1..];
        }
    }
    Ok(modules)
}

pub(crate) fn find_modules(dir: &Path) -> Result<Vec<GoModule>> {
    let dir = dir.canonicalize()?;
    go_list(&dir)
}

pub(crate) fn get_project_root() -> Result<PathBuf> {
    if let Ok(Some(first_module)) = go_list(Path::new(".")).map(|v| v.into_iter().next()) {
        Ok(first_module.root)
    } else {
        Ok(Path::new(".").canonicalize()?)
    }
}
