use crate::{GoImportPath, GoTestArtifact};
use anyhow::{anyhow, Context as _, Result};
use maelstrom_test_runner::ui::UiSender;
use maelstrom_util::fs::Fs;
use maelstrom_util::process::ExitCode;
use serde::Deserialize;
use std::os::unix::process::ExitStatusExt as _;
use std::{
    fmt,
    io::{BufRead, BufReader},
    path::Path,
    path::PathBuf,
    process::{Command, ExitStatus, Stdio},
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

fn handle_build_output(ui: UiSender, send_to_ui: bool, r: impl BufRead) -> Result<String> {
    let mut output_s = String::new();
    for line in r.lines() {
        let line = line?;
        output_s += &line;
        output_s += "\n";
        if send_to_ui {
            ui.build_output_line(line);
        }
    }
    Ok(output_s)
}

fn handle_build_cmd_status(
    exit_status: ExitStatus,
    stdout: String,
    stderr: String,
) -> Result<String> {
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

fn run_build_cmd(cmd: &mut Command, stdout_to_ui: bool, ui: UiSender) -> Result<String> {
    let mut child = cmd.stderr(Stdio::piped()).stdout(Stdio::piped()).spawn()?;

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let ui_clone = ui.clone();
    let stdout_handle = thread::spawn(move || handle_build_output(ui_clone, stdout_to_ui, stdout));

    let stderr = BufReader::new(child.stderr.take().unwrap());
    let ui_clone = ui.clone();
    let stderr_handle = thread::spawn(move || handle_build_output(ui_clone, true, stderr));

    let stdout = stdout_handle.join().unwrap()?;
    let stderr = stderr_handle.join().unwrap()?;

    let exit_status = child.wait()?;
    handle_build_cmd_status(exit_status, stdout, stderr)
}

fn go_build(dir: &Path, ui: UiSender) -> Result<String> {
    run_build_cmd(
        Command::new("go").current_dir(dir).arg("test").arg("-c"),
        true, /* stdout_to_ui */
        ui,
    )
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
    packages: Vec<GoPackage>,
    send: mpsc::Sender<GoTestArtifact>,
    ui: UiSender,
) -> Result<()> {
    let mut handles = vec![];
    for m in packages {
        let send_clone = send.clone();
        let ui_clone = ui.clone();
        handles.push(thread::spawn(move || -> Result<()> {
            let res = go_build(&m.dir, ui_clone);
            if is_no_go_files_error(&res) {
                return Ok(());
            }

            let output = res?;
            if !output.contains("[no test files]") {
                let import_path = GoImportPath(m.import_path.clone());
                let _ = send_clone.send(GoTestArtifact {
                    name: m.name.clone(),
                    path: m.dir.join(format!("{}.test", import_path.short_name())),
                    id: import_path,
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
    packages: Vec<&GoPackage>,
    ui: UiSender,
) -> Result<(WaitHandle, TestArtifactStream)> {
    let paths = packages.into_iter().cloned().collect();
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
pub(crate) struct GoPackage {
    pub dir: PathBuf,
    pub import_path: String,
    pub name: String,
    #[allow(dead_code)]
    pub root: PathBuf,
}

pub(crate) fn go_list(dir: &Path, ui: &UiSender) -> Result<Vec<GoPackage>> {
    let stdout = run_build_cmd(
        Command::new("go")
            .current_dir(dir)
            .arg("list")
            .arg("-json")
            .arg("./..."),
        false, /* stdout_to_ui */
        ui.clone(),
    )?
    .into_bytes();

    let mut packages = vec![];
    let mut cursor = &stdout[..];
    while !cursor.is_empty() {
        let mut d = serde_json::Deserializer::new(serde_json::de::IoRead::new(&mut cursor));
        let m: GoPackage = serde::Deserialize::deserialize(&mut d)?;
        packages.push(m);
        while !cursor.is_empty() && (cursor[0] as char).is_ascii_whitespace() {
            cursor = &cursor[1..];
        }
    }
    Ok(packages)
}

/// Find the module root by searching upwards from PWD to a path containing a "go.mod"
pub(crate) fn get_module_root() -> Result<PathBuf> {
    let mut c = Path::new(".").canonicalize()?;

    while !Fs.exists(c.join("go.mod")) {
        c = c
            .parent()
            .ok_or_else(|| anyhow!("Failed to find module root"))?
            .into();
    }
    Ok(c)
}
