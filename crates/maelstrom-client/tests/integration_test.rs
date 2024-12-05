use assert_matches::assert_matches;
use maelstrom_base::{
    JobCompleted, JobEffects, JobMount, JobNetwork, JobOutcome, JobOutputResult,
    JobTerminationStatus, Utf8Path, Utf8PathBuf,
};
use maelstrom_client::{
    AcceptInvalidRemoteContainerTlsCerts, ArtifactUploadStrategy, CacheDir, Client,
    ClientBgProcess, ContainerImageDepotDir, ContainerSpec, JobSpec, LayerSpec, PrefixOptions,
    ProjectDir, StateDir, SymlinkSpec,
};
use maelstrom_util::{elf::read_shared_libraries, fs::Fs, log::test_logger, root::Root};
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
        maelstrom_client::bg_proc_main().unwrap();
    }

    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_maelstrom-client"));
    ClientBgProcess::new_from_bin(&bin_path, &[]).unwrap()
}

struct ClientFixture {
    client: Client,
    layers: Vec<LayerSpec>,
    self_path: Utf8PathBuf,
    test_line: u32,
    temp_dir: tempfile::TempDir,
    fs: Fs,
}

impl ClientFixture {
    fn new() -> Self {
        let fs = Fs::new();
        let temp_dir = tempdir().unwrap();
        let project_dir = temp_dir.path().join("project");
        fs.create_dir_all(&project_dir).unwrap();
        let cache_dir = temp_dir.path().join("cache");
        fs.create_dir_all(&cache_dir).unwrap();
        let state_dir = temp_dir.path().join("state");
        fs.create_dir_all(&state_dir).unwrap();
        let container_image_depot_dir = temp_dir.path().join("container_image_depot");
        fs.create_dir_all(&container_image_depot_dir).unwrap();

        let bg_proc = spawn_bg_proc();
        let log = test_logger();
        slog::info!(log, "connected unix socket to child");
        let client = Client::new(
            bg_proc,
            None, /* broker_addr */
            Root::<ProjectDir>::new(&project_dir),
            Root::<StateDir>::new(&state_dir),
            Root::<ContainerImageDepotDir>::new(&container_image_depot_dir),
            Root::<CacheDir>::new(&cache_dir),
            "1mb".parse().unwrap(), /* cache_size */
            "1mb".parse().unwrap(), /* inline_limit */
            2u16.try_into().unwrap(),
            AcceptInvalidRemoteContainerTlsCerts::from(true),
            ArtifactUploadStrategy::TcpUpload,
            log.clone(),
        )
        .unwrap();
        slog::info!(log, "client connected via RPC");

        let mut layers = vec![];
        let self_path = fs.read_link("/proc/self/exe").unwrap();
        let sos = read_shared_libraries(&self_path).unwrap();
        layers.push(LayerSpec::Paths {
            paths: sos
                .into_iter()
                .map(|p| Utf8PathBuf::from_path_buf(p).unwrap())
                .collect(),
            prefix_options: PrefixOptions {
                strip_prefix: Some("/".into()),
                follow_symlinks: true,
                ..Default::default()
            },
        });

        let self_path = Utf8PathBuf::from_path_buf(self_path).unwrap();
        layers.push(LayerSpec::Paths {
            paths: vec![self_path.clone()],
            prefix_options: PrefixOptions::default(),
        });
        Self {
            fs,
            client,
            layers,
            self_path,
            test_line: 0,
            temp_dir,
        }
    }

    fn run_job(&self, added_layers: Vec<LayerSpec>) -> String {
        let mut layers = self.layers.clone();
        layers.extend(added_layers);
        let spec = JobSpec::new(self.self_path.clone(), layers)
            .arguments(["--exact", "single_test", "--nocapture"])
            .environment([
                ("INSIDE_JOB", "yes"),
                ("TEST_LINE", &self.test_line.to_string()),
            ]);
        let (_, outcome) = self.client.run_job(spec).unwrap();
        let output = assert_matches!(
            outcome,
            Ok(JobOutcome::Completed(JobCompleted {
                status: JobTerminationStatus::Exited(0),
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

    fn add_container_expecting_error(
        &self,
        name: String,
        layers: Vec<LayerSpec>,
        mounts: Vec<JobMount>,
        network: JobNetwork,
    ) -> anyhow::Error {
        let spec = ContainerSpec {
            image: None,
            layers,
            root_overlay: Default::default(),
            environment: vec![],
            working_directory: None,
            mounts,
            network,
            user: None,
            group: None,
        };
        self.client.add_container(name, spec).unwrap_err()
    }

    fn run_job_expecting_error(
        &self,
        added_layers: Vec<LayerSpec>,
        mounts: Vec<JobMount>,
        network: JobNetwork,
    ) -> anyhow::Error {
        let mut layers = self.layers.clone();
        layers.extend(added_layers);
        let spec = JobSpec::new(self.self_path.clone(), layers)
            .mounts(mounts)
            .network(network)
            .arguments(["--exact", "single_test", "--nocapture"])
            .environment([
                ("INSIDE_JOB", "yes"),
                ("TEST_LINE", &self.test_line.to_string()),
            ]);
        self.client.run_job(spec).unwrap_err()
    }
}

struct Fixture {
    client_fixture: Option<ClientFixture>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            client_fixture: (std::env::var("INSIDE_JOB").unwrap_or_default() != "yes")
                .then(ClientFixture::new),
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

    let layer = LayerSpec::Tar {
        path: Utf8PathBuf::from_path_buf(tar_path.clone()).unwrap(),
    };
    let output = fix.run_job(vec![layer]);
    assert_eq!(output, "hello world\n");
}

fn tar_test_job() {
    let fs = Fs::new();

    let contents = fs.read_to_string("/foo.bin").unwrap();
    println!("{contents}");
}

fn paths_test(fix: &ClientFixture, create: &[&str], layer: LayerSpec, expected: &[&str]) {
    let root = Utf8PathBuf::from_path_buf(fix.temp_dir.path().into()).unwrap();

    for path in create {
        let path = Utf8Path::new(path);
        if let Some(parent) = path.parent() {
            fix.fs.create_dir_all(root.join(parent)).unwrap();
        }
        fix.fs.write(root.join(path), b"").unwrap();
    }

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
        let meta = fs.symlink_metadata(path).unwrap();
        if meta.is_symlink() {
            let data = fs.read_link(path).unwrap();
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
        LayerSpec::Paths {
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
        LayerSpec::Paths {
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
        LayerSpec::Paths {
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
        LayerSpec::Glob {
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
        LayerSpec::Stubs {
            stubs: vec!["/foo/{bar,baz}".into(), "/foo/qux/".into()],
        },
        &["/foo/", "/foo/bar", "/foo/baz", "/foo/qux/"],
    )
}

fn symlinks_test(fix: &ClientFixture) {
    paths_test(
        fix,
        &[],
        LayerSpec::Symlinks {
            symlinks: vec![SymlinkSpec {
                link: "/foo".into(),
                target: "/bar".into(),
            }],
        },
        &["/foo => /bar"],
    )
}

fn sys_local_network_error_test(fix: &ClientFixture) {
    let error1 = fix.run_job_expecting_error(
        vec![LayerSpec::Stubs {
            stubs: vec!["/sys/".into()],
        }],
        vec![JobMount::Sys {
            mount_point: "/sys".into(),
        }],
        JobNetwork::Local,
    );
    let error2 = fix.add_container_expecting_error(
        "my_container".into(),
        vec![LayerSpec::Stubs {
            stubs: vec!["/sys/".into()],
        }],
        vec![JobMount::Sys {
            mount_point: "/sys".into(),
        }],
        JobNetwork::Local,
    );
    for error in [error1, error2] {
        assert!(error.to_string().contains(
            "A \"sys\" mount is not compatible with local networking. \
            Check the documentation for the \"network\" field of \"JobSpec\"."
        ));
    }
}

fn panic_test_job() {
    panic!("this job wasn't expected to run");
}

/// Starting up the local-worker in the dev profile can be slow, so just run all the tests with the
/// one local-worker to speed things up.
#[test]
fn single_test() {
    let mut fix = Fixture::new();

    fix.run_test(tar_test, tar_test_job);
    fix.run_test(paths_test_strip_prefix, paths_test_job);
    fix.run_test(paths_test_prepend_prefix, paths_test_job);
    fix.run_test(paths_test_absolute, paths_test_job);
    fix.run_test(glob_test, paths_test_job);
    fix.run_test(stubs_test, paths_test_job);
    fix.run_test(symlinks_test, paths_test_job);
    fix.run_test(sys_local_network_error_test, panic_test_job);
}
