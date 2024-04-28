use assert_matches::assert_matches;
use maelstrom_base::{GroupId, JobCompleted, JobOutcome, JobSpec, JobStatus, UserId, Utf8PathBuf};
use maelstrom_client::{Client, ClientBgProcess};
use maelstrom_client_base::spec::{Layer, PrefixOptions};
use maelstrom_util::elf::read_shared_libraries;
use maelstrom_util::fs::Fs;
use slog::Drain as _;
use std::path::PathBuf;
use tempfile::tempdir;

fn test_logger() -> slog::Logger {
    let decorator = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

fn spawn_bg_proc() -> ClientBgProcess {
    // XXX cargo-maelstrom doesn't add shared-library dependencies for additional binaries.
    //
    // To make us have the same dependencies as the client-process, call into the client-process
    // code in some code-path which won't execute but the compiler won't optimize out.
    if std::env::args().next().unwrap() == "not_going_to_happen" {
        let (a, _) = std::os::unix::net::UnixStream::pair().unwrap();
        maelstrom_client_process::main(a, None).unwrap();
    }

    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_maelstrom-client"));
    ClientBgProcess::new_from_bin(&bin_path).unwrap()
}

#[test]
fn simple() {
    let fs = Fs::new();
    let tmp_dir = tempdir().unwrap();
    let project_dir = tmp_dir.path().join("project");
    fs.create_dir_all(&project_dir).unwrap();
    let cache_dir = tmp_dir.path().join("cache");
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
        "1kb".parse().unwrap(), /* inline_limit */
        2u16.try_into().unwrap(),
        log.clone(),
    )
    .unwrap();
    slog::info!(log, "client connected via RPC");

    let self_path = fs.read_link("/proc/self/exe").unwrap();
    let sos = read_shared_libraries(&self_path).unwrap();
    let layer1 = client
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
        .unwrap();

    let self_path = Utf8PathBuf::from_path_buf(self_path).unwrap();
    let layer2 = client
        .add_layer(Layer::Paths {
            paths: vec![self_path.clone()],
            prefix_options: PrefixOptions::default(),
        })
        .unwrap();

    let spec = JobSpec {
        program: self_path,
        arguments: vec!["--exact".into(), "helper".into(), "--nocapture".into()],
        environment: vec![],
        layers: vec![layer1, layer2].try_into().unwrap(),
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
    client
        .add_job(spec, move |_, outcome| send.send(outcome).unwrap())
        .unwrap();
    let outcome = recv.recv().unwrap();
    assert_matches!(
        outcome,
        Ok(JobOutcome::Completed(JobCompleted {
            status: JobStatus::Exited(0),
            ..
        }))
    );
}

#[test]
fn helper() {
    // todo
}
