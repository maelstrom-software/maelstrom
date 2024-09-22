use crate::{GoImportPath, GoTestOptions};
use anyhow::{anyhow, Context as _, Result};
use maelstrom_linux as linux;
use maelstrom_test_runner::{ui::UiWeakSender, BuildDir};
use maelstrom_util::{
    ext::BoolExt as _,
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
};
use serde::Deserialize;
use std::collections::HashSet;
use std::os::unix::process::ExitStatusExt as _;
use std::{
    fmt,
    io::{BufRead, BufReader},
    path::Path,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    str,
    sync::{mpsc, Arc, Mutex},
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

#[derive(Default)]
struct MultiProcessKillerState {
    children: HashSet<linux::Pid>,
    killing: bool,
}

#[derive(Default)]
struct MultiProcessKiller {
    state: Mutex<MultiProcessKillerState>,
}

impl MultiProcessKiller {
    fn add_child(&self, child: &Child) {
        let pid = linux::Pid::from(child);
        let mut state = self.state.lock().unwrap();
        if state.killing {
            let _ = linux::kill(pid, linux::Signal::KILL);
        } else {
            state.children.insert(pid).assert_is_true();
        }
    }

    fn remove_child(&self, child: &Child) {
        let pid = linux::Pid::from(child);
        let mut state = self.state.lock().unwrap();
        let _ = state.children.remove(&pid);
    }

    fn kill(&self) {
        let mut state = self.state.lock().unwrap();
        for pid in std::mem::take(&mut state.children) {
            let _ = linux::kill(pid, linux::Signal::KILL);
        }
    }

    fn is_killing(&self) -> bool {
        self.state.lock().unwrap().killing
    }
}

pub struct WaitHandle {
    handle: Mutex<Option<thread::JoinHandle<Result<()>>>>,
    killer: Arc<MultiProcessKiller>,
}

impl WaitHandle {
    pub fn wait(&self) -> Result<()> {
        let mut locked_handle = self.handle.lock().unwrap();
        let handle = locked_handle.take().expect("wait only called once");
        handle.join().unwrap()
    }
    pub fn kill(&self) -> Result<()> {
        self.killer.kill();
        Ok(())
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

fn handle_build_output(ui: UiWeakSender, send_to_ui: bool, r: impl BufRead) -> Result<String> {
    let mut output_s = String::new();
    for line in r.lines() {
        let line = line?;
        output_s += &line;
        output_s += "\n";
        if send_to_ui {
            if let Some(ui) = ui.upgrade() {
                ui.build_output_line(line);
            } else {
                break;
            }
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

fn run_build_cmd(
    killer: &MultiProcessKiller,
    cmd: &mut Command,
    stdout_to_ui: bool,
    ui: UiWeakSender,
) -> Result<String> {
    let mut child = cmd.stderr(Stdio::piped()).stdout(Stdio::piped()).spawn()?;

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let ui_clone = ui.clone();
    let stdout_handle = thread::spawn(move || handle_build_output(ui_clone, stdout_to_ui, stdout));

    let stderr = BufReader::new(child.stderr.take().unwrap());
    let ui_clone = ui.clone();
    let stderr_handle = thread::spawn(move || handle_build_output(ui_clone, true, stderr));

    killer.add_child(&child);

    let stdout = stdout_handle.join().unwrap()?;
    let stderr = stderr_handle.join().unwrap()?;

    let exit_status = child.wait()?;

    killer.remove_child(&child);

    handle_build_cmd_status(exit_status, stdout, stderr)
}

fn go_build(
    killer: Arc<MultiProcessKiller>,
    dir: &Path,
    output: &Path,
    options: &GoTestOptions,
    ui: UiWeakSender,
) -> Result<String> {
    let mut cmd = Command::new("go");
    cmd.current_dir(dir)
        .arg("test")
        .arg("-c")
        .arg("-o")
        .arg(output);
    if let Some(vet_value) = &options.vet {
        cmd.arg(format!("--vet={vet_value}"));
    }
    run_build_cmd(&killer, &mut cmd, true /* stdout_to_ui */, ui)
}

fn is_no_go_files_error<V>(res: &Result<V>) -> bool {
    if let Err(e) = res {
        if let Some(e) = e.downcast_ref::<BuildError>() {
            return e.stderr.contains("no Go files");
        }
    }
    false
}

pub(crate) struct GoTestArtifact {
    pub package: GoPackage,
    pub path: PathBuf,
    pub options: GoTestOptions,
}

fn multi_go_build(
    killer: Arc<MultiProcessKiller>,
    packages: Vec<GoPackage>,
    build_dir: RootBuf<BuildDir>,
    options: GoTestOptions,
    send: mpsc::Sender<GoTestArtifact>,
    ui: UiWeakSender,
) -> Result<()> {
    let build_dir = build_dir.into_path_buf();

    let mut handles = vec![];
    for pkg in packages {
        if killer.is_killing() {
            break;
        }

        // We put the built binary in a deterministic location so rebuilds overwrite what was there
        // before
        let import_path = GoImportPath(pkg.import_path.clone());
        let binary_name = format!("{}.test", import_path.short_name());
        let binary_path = build_dir.join(pkg.root_relative_path()).join(binary_name);
        if let Some(parent) = binary_path.parent() {
            Fs.create_dir_all(parent)?;
        }

        let send_clone = send.clone();
        let ui_clone = ui.clone();
        let options_clone = options.clone();
        let killer_clone = killer.clone();
        handles.push(thread::spawn(move || -> Result<()> {
            let res = go_build(
                killer_clone,
                &pkg.dir,
                &binary_path,
                &options_clone,
                ui_clone,
            );
            if is_no_go_files_error(&res) {
                return Ok(());
            }

            let output = res?;
            if !output.contains("[no test files]") {
                let _ = send_clone.send(GoTestArtifact {
                    package: pkg,
                    path: binary_path.to_path_buf(),
                    options: options_clone,
                });
            }
            Ok(())
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    if let Some(ui) = ui.upgrade() {
        ui.done_building();
    }

    for res in results {
        res?
    }

    Ok(())
}

pub(crate) fn build_and_collect(
    options: &GoTestOptions,
    packages: Vec<&GoPackage>,
    build_dir: &Root<BuildDir>,
    ui: UiWeakSender,
) -> Result<(WaitHandle, TestArtifactStream)> {
    let options = options.clone();
    let build_dir = build_dir.to_owned();
    let paths = packages.into_iter().cloned().collect();
    let killer = Arc::new(MultiProcessKiller::default());
    let cloned_killer = killer.clone();
    let (send, recv) = mpsc::channel();
    let handle =
        thread::spawn(move || multi_go_build(cloned_killer, paths, build_dir, options, send, ui));
    Ok((
        WaitHandle {
            handle: Mutex::new(Some(handle)),
            killer,
        },
        TestArtifactStream { recv },
    ))
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
    pub root: PathBuf,
}

impl GoPackage {
    pub fn root_relative_path(&self) -> PathBuf {
        self.dir
            .strip_prefix(&self.root)
            .map(|p| p.to_path_buf())
            .unwrap_or_default()
    }
}

pub(crate) fn go_list(dir: &Path, ui: UiWeakSender) -> Result<Vec<GoPackage>> {
    let stdout = run_build_cmd(
        &MultiProcessKiller::default(),
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
        let pkg: GoPackage = serde::Deserialize::deserialize(&mut d)?;
        packages.push(pkg);
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
