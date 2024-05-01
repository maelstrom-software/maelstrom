use assert_matches::assert_matches;
use maelstrom_base::{
    ArtifactType, GroupId, JobCompleted, JobEffects, JobOutcome, JobOutputResult, JobSpec,
    JobStatus, Sha256Digest, UserId, Utf8Path, Utf8PathBuf,
};
use maelstrom_client::{Client, ClientBgProcess};
use maelstrom_client_base::spec::{Layer, PrefixOptions, SymlinkSpec};
use maelstrom_util::{
    config::common::LogLevel,
    elf::read_shared_libraries,
    fs::Fs,
    log::{test_logger, LoggerFactory},
};
use regex::Regex;
use std::panic::Location;
use std::path::PathBuf;
use tempfile::tempdir;

fn spawn_bg_proc() -> ClientBgProcess {
    // XXX cargo-maelstrom doesn't add shared-library dependencies for additional binaries.
    //
    // To make us have the same dependencies as the client-process, call into the client-process
    // code in some code-path which won't execute but the compiler won't optimize out.
    if std::env::args().next().unwrap() == "not_going_to_happen" {
        let (a, _) = std::os::unix::net::UnixStream::pair().unwrap();
        maelstrom_client_process::main(a, LoggerFactory::FromLevel(LogLevel::Debug)).unwrap();
    }

    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_maelstrom-client"));
    ClientBgProcess::new_from_bin(&bin_path).unwrap()
}

struct ClientFixture {
    client: Client,
    layers: Vec<(Sha256Digest, ArtifactType)>,
    self_path: Utf8PathBuf,
    test_line: u32,
    temp_dir: tempfile::TempDir,
    fs: Fs,
    log: slog::Logger,
}

impl ClientFixture {
    fn new() -> Self {
        let fs = Fs::new();
        let temp_dir = tempdir().unwrap();
        let project_dir = temp_dir.path().join("project");
        fs.create_dir_all(&project_dir).unwrap();
        let cache_dir = temp_dir.path().join("cache");
        fs.create_dir_all(&cache_dir).unwrap();

        let bg_proc = spawn_bg_proc();
        let log = test_logger();
        slog::info!(log, "connected unix socket to child");
        let client = Client::new(
            bg_proc,
            None, /* broker_addr */
            &project_dir,
            &cache_dir,
            "1mb".parse().unwrap(), /* cache_size */
            "1mb".parse().unwrap(), /* inline_limit */
            2u16.try_into().unwrap(),
            log.clone(),
        )
        .unwrap();
        slog::info!(log, "client connected via RPC");

        let mut layers = vec![];
        let self_path = fs.read_link("/proc/self/exe").unwrap();
        let sos = read_shared_libraries(&self_path).unwrap();
        layers.push(
            client
                .add_layer(Layer::Paths {
                    paths: sos
                        .into_iter()
                        .map(|p| Utf8PathBuf::from_path_buf(p).unwrap())
                        .collect(),
                    prefix_options: PrefixOptions {
                        strip_prefix: Some("/".into()),
                        follow_symlinks: true,
                        ..Default::default()
                    },
                })
                .unwrap(),
        );

        let self_path = Utf8PathBuf::from_path_buf(self_path).unwrap();
        layers.push(
            client
                .add_layer(Layer::Paths {
                    paths: vec![self_path.clone()],
                    prefix_options: PrefixOptions::default(),
                })
                .unwrap(),
        );
        Self {
            fs,
            client,
            layers,
            self_path,
            test_line: 0,
            temp_dir,
            log,
        }
    }

    fn run_job(&self, added_layers: Vec<(Sha256Digest, ArtifactType)>) -> String {
        let mut layers = self.layers.clone();
        layers.extend(added_layers);
        let spec = JobSpec {
            program: self.self_path.clone(),
            arguments: vec!["--exact".into(), "single_test".into(), "--nocapture".into()],
            environment: vec![
                "INSIDE_JOB=yes".into(),
                format!("TEST_LINE={}", self.test_line),
            ],
            layers: layers.try_into().unwrap(),
            devices: Default::default(),
            mounts: vec![],
            enable_loopback: false,
            enable_writable_file_system: false,
            working_directory: "/".into(),
            user: UserId::new(0),
            group: GroupId::new(0),
            timeout: None,
        };
        let (send, recv) = std::sync::mpsc::channel();
        self.client
            .add_job(spec, move |_, outcome| send.send(outcome).unwrap())
            .unwrap();
        let outcome = recv.recv().unwrap();
        let output = assert_matches!(
            outcome,
            Ok(JobOutcome::Completed(JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects { stdout: JobOutputResult::Inline(stdout), .. },
                ..
            })) => stdout
        );
        let output = std::str::from_utf8(&output).unwrap();
        let output_re =
            Regex::new("(?s)^\nrunning 1 test\n(.*)test .* \\.\\.\\. ok\n\n.*$").unwrap();
        let captured = output_re.captures(output).unwrap().get(1).unwrap();
        captured.as_str().to_owned()
    }
}

impl Drop for ClientFixture {
    fn drop(&mut self) {
        slog::debug!(self.log, "dropping ClientFixture");
        self.client.wait_for_outstanding_jobs().unwrap();
    }
}

struct Fixture {
    client_fixture: Option<ClientFixture>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            client_fixture: (std::env::var("INSIDE_JOB").unwrap_or_default() != "yes")
                .then(|| ClientFixture::new()),
        }
    }

    #[track_caller]
    fn run_test(&mut self, run_job: impl FnOnce(&ClientFixture), inside_job: impl FnOnce()) {
        let test_line = Location::caller().line();
        if let Some(client_fixture) = &mut self.client_fixture {
            client_fixture.test_line = test_line;
            run_job(client_fixture)
        } else {
            let test_line_to_run: u32 = std::env::var("TEST_LINE").unwrap().parse().unwrap();
            if test_line_to_run == test_line {
                inside_job()
            }
        }
    }
}

fn tar_test(fix: &ClientFixture) {
    let tar_path = fix.temp_dir.path().join("test.tar");
    let mut tar = tar::Builder::new(fix.fs.create_file(&tar_path).unwrap());
    let mut header = tar::Header::new_gnu();

    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(11);
    header.set_mode(0o555);
    tar.append_data(&mut header, "foo.bin", b"hello world".as_slice())
        .unwrap();
    tar.finish().unwrap();

    let layer = fix
        .client
        .add_layer(Layer::Tar {
            path: Utf8PathBuf::from_path_buf(tar_path.clone()).unwrap(),
        })
        .unwrap();
    let output = fix.run_job(vec![layer]);
    assert_eq!(output, "hello world\n");
}

fn tar_test_job() {
    let fs = Fs::new();

    let contents = fs.read_to_string("/foo.bin").unwrap();
    println!("{contents}");
}

fn paths_test(fix: &ClientFixture, create: &[&str], layer: Layer, expected: &[&str]) {
    let root = Utf8PathBuf::from_path_buf(fix.temp_dir.path().into()).unwrap();

    for path in create {
        let path = Utf8Path::new(path);
        if let Some(parent) = path.parent() {
            fix.fs.create_dir_all(root.join(parent)).unwrap();
        }
        fix.fs.write(root.join(path), b"").unwrap();
    }
    let layer = fix.client.add_layer(layer).unwrap();

    let mut output: Vec<String> = serde_json::from_str(&fix.run_job(vec![layer])).unwrap();
    output.sort();
    let filtered = Vec::from_iter(output.into_iter().filter(|e| {
        !e.starts_with("/home") && !e.starts_with("/nix") && !e.starts_with("/integration_test-")
    }));

    assert_eq!(filtered, expected);
}

fn paths_test_job() {
    let fs = Fs::new();

    let mut output = vec![];

    for e in fs.walk("/") {
        let e = e.unwrap();
        let path = e.to_str().unwrap();
        if path == "/" {
            continue;
        }
        let mut str_e = path.to_owned();
        let meta = fs.symlink_metadata(&path).unwrap();
        if meta.is_symlink() {
            let data = fs.read_link(&path).unwrap();
            str_e += &format!(" => {}", data.to_str().unwrap());
        } else if meta.is_dir() {
            str_e += "/";
        }
        output.push(str_e);
    }
    println!("{}", serde_json::to_string(&output).unwrap());
}

fn paths_test_strip_prefix(fix: &ClientFixture) {
    paths_test(
        fix,
        &["project/a/foo.bin", "project/a/bar.bin"],
        Layer::Paths {
            paths: vec!["a/foo.bin".into(), "a/bar.bin".into()],
            prefix_options: PrefixOptions {
                strip_prefix: Some("a".into()),
                ..Default::default()
            },
        },
        &["/bar.bin", "/foo.bin"],
    )
}

fn paths_test_prepend_prefix(fix: &ClientFixture) {
    paths_test(
        fix,
        &["project/foo2.bin", "project/bar2.bin"],
        Layer::Paths {
            paths: vec!["foo2.bin".into(), "bar2.bin".into()],
            prefix_options: PrefixOptions {
                prepend_prefix: Some("/baz".into()),
                ..Default::default()
            },
        },
        &["/baz/", "/baz/bar2.bin", "/baz/foo2.bin"],
    )
}

fn paths_test_absolute(fix: &ClientFixture) {
    let root = Utf8PathBuf::from_path_buf(fix.temp_dir.path().into()).unwrap();
    paths_test(
        fix,
        &["foo3.bin", "bar3.bin"],
        Layer::Paths {
            paths: vec![root.join("foo3.bin"), root.join("bar3.bin")],
            prefix_options: PrefixOptions {
                strip_prefix: Some(root),
                ..Default::default()
            },
        },
        &["/bar3.bin", "/foo3.bin"],
    )
}

fn glob_test(fix: &ClientFixture) {
    paths_test(
        fix,
        &["project/foo.txt", "project/bar.bin"],
        Layer::Glob {
            glob: "*.txt".into(),
            prefix_options: Default::default(),
        },
        &["/foo.txt"],
    )
}

fn stubs_test(fix: &ClientFixture) {
    paths_test(
        fix,
        &[],
        Layer::Stubs {
            stubs: vec!["/foo/{bar,baz}".into(), "/foo/qux/".into()],
        },
        &["/foo/", "/foo/bar", "/foo/baz", "/foo/qux/"],
    )
}

fn symlinks_test(fix: &ClientFixture) {
    paths_test(
        fix,
        &[],
        Layer::Symlinks {
            symlinks: vec![SymlinkSpec {
                link: "/foo".into(),
                target: "/bar".into(),
            }],
        },
        &["/foo => /bar"],
    )
}

/// Starting up the local-worker in the dev profile can be slow, so just run all the tests with the
/// one local-worker to speed things up.
#[test]
fn single_test() {
    let mut fix = Fixture::new();

    fix.run_test(|fix| tar_test(fix), || tar_test_job());
    fix.run_test(|fix| paths_test_strip_prefix(fix), || paths_test_job());
    fix.run_test(|fix| paths_test_prepend_prefix(fix), || paths_test_job());
    fix.run_test(|fix| paths_test_absolute(fix), || paths_test_job());
    fix.run_test(|fix| glob_test(fix), || paths_test_job());
    fix.run_test(|fix| stubs_test(fix), || paths_test_job());
    fix.run_test(|fix| symlinks_test(fix), || paths_test_job());
}
