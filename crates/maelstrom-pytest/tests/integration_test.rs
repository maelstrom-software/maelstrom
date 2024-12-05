use anyhow::{bail, Result};
use indicatif::InMemoryTerm;
use maelstrom_client::{ArtifactUploadStrategy, ClientBgProcess, ProjectDir};
use maelstrom_container::local_registry;
use maelstrom_pytest::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_test_runner::ui;
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempfile::tempdir;

fn spawn_bg_proc() -> ClientBgProcess {
    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_maelstrom-pytest"));
    ClientBgProcess::new_from_bin(&bin_path, &["--client-bg-proc"]).unwrap()
}

fn sh(script: &str, description: &str) -> Result<()> {
    let mut cmd = Command::new("/bin/sh");
    cmd.args(["-c", script])
        .stderr(Stdio::piped())
        .stdout(Stdio::null());
    let mut child = cmd.spawn()?;

    let mut stderr = child.stderr.take().unwrap();
    let stderr_handle = std::thread::spawn(move || -> String {
        let mut stderr_buffer = vec![];
        let _ = std::io::copy(&mut stderr, &mut stderr_buffer);
        String::from_utf8_lossy(&stderr_buffer).into()
    });

    let exit_status = child.wait()?;
    if exit_status.success() {
        Ok(())
    } else {
        let stderr_str = stderr_handle.join().unwrap();
        bail!("{description} failed with: {exit_status}, and stderr = {stderr_str}")
    }
}

fn maybe_install_pytest() {
    if sh("python -c 'import pytest'", "").is_err() {
        sh("pip install pytest==8.1.1", "pip install").unwrap()
    }
}

fn do_maelstrom_pytest_test(
    source_contents: &str,
    extra_options: ExtraCommandLineOptions,
    terminal_size: (u16, u16),
) -> (String, String, ExitCode) {
    maybe_install_pytest();

    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");
    fs.create_dir(&project_dir).unwrap();
    fs.write(project_dir.join("test_foo.py"), source_contents)
        .unwrap();

    fs.write(project_dir.join("test-requirements.txt"), "pytest==8.1.1")
        .unwrap();

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let log = maelstrom_util::log::test_logger();
    let container_address =
        local_registry::LocalRegistry::run_sync(manifest_dir.join("src"), log.clone()).unwrap();

    fs.write(
        project_dir.join("maelstrom-pytest.toml"),
        format!(
            indoc::indoc! {r#"
                [[directives]]
                image.name = "docker://{container_address}/python"
                image.use = ["layers", "environment"]
                added_layers = [
                    {{ glob = "**.py" }},
                    {{ stubs = ["/tmp/", "/proc/", "/sys/", "/dev/null", "/.pytest_cache/"] }}
                ]
                mounts = [
                    {{ type = "tmp", mount_point = "/tmp" }},
                    {{ type = "tmp", mount_point = "/.pytest_cache" }},
                    {{ type = "proc", mount_point = "/proc" }},
                    {{ type = "devices", devices = ["null"] }},
                ]
            "#},
            container_address = container_address
        ),
    )
    .unwrap();

    let container_image_depot_root = RootBuf::new(temp_dir.path().join("container"));

    let config = Config {
        parent: maelstrom_test_runner::config::Config {
            broker: None,
            log_level: LogLevel::Debug,
            container_image_depot_root,
            timeout: None,
            cache_size: CacheSize::default(),
            inline_limit: InlineLimit::default(),
            slots: Slots::default(),
            accept_invalid_remote_container_tls_certs: true.into(),
            ui: ui::UiKind::Simple,
            repeat: Default::default(),
            stop_after: None,
            artifact_upload_strategy: ArtifactUploadStrategy::TcpUpload,
        },
        pytest_options: Default::default(),
    };
    let term = InMemoryTerm::new(terminal_size.0, terminal_size.1);

    let logger = Logger::GivenLogger(log.clone());

    let ui = ui::SimpleUi::new(extra_options.list, false, term.clone());
    let mut stderr = vec![];
    let bg_proc = spawn_bg_proc();
    let exit_code = maelstrom_pytest::main_with_stderr_and_project_dir(
        config,
        extra_options,
        bg_proc,
        logger,
        false,
        ui,
        &mut stderr,
        Root::<ProjectDir>::new(&project_dir),
    )
    .unwrap();

    (
        term.contents(),
        String::from_utf8(stderr).unwrap(),
        exit_code,
    )
}

fn do_maelstrom_pytest_test_success(
    source_contents: &str,
    extra_options: ExtraCommandLineOptions,
    terminal_size: (u16, u16),
) -> String {
    let (contents, stderr, exit_code) =
        do_maelstrom_pytest_test(source_contents, extra_options, terminal_size);
    assert_eq!(exit_code, ExitCode::SUCCESS);
    assert_eq!(stderr, "");
    contents
}

#[test]
fn test_simple_success() {
    let contents = do_maelstrom_pytest_test_success(
        &indoc::indoc! {"
            def test_noop():
                pass
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: false,
        },
        (50, 50),
    );
    assert!(
        contents.contains("test_foo.py::test_noop.................OK"),
        "{contents}"
    );
    assert!(
        contents.ends_with(
            "\
            ================== Test Summary ==================\n\
            Successful Tests:         1\n\
            Failed Tests    :         0\
        "
        ),
        "{contents}"
    );
}

#[test]
fn test_simple_failure() {
    let (contents, stderr, exit_code) = do_maelstrom_pytest_test(
        &indoc::indoc! {"
            def test_error():
                raise Exception('test error')
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: false,
        },
        (50, 50),
    );
    assert_eq!(stderr, "");
    assert_eq!(exit_code, ExitCode::from(1));

    let first_line = contents.split("\n").next().unwrap();
    let rest = &contents[first_line.len() + 1..];

    assert!(
        first_line.starts_with("test_foo.py::test_error..............FAIL"),
        "{contents}"
    );
    assert_eq!(
        rest,
        indoc::indoc! {"

                def test_error():
            >       raise Exception('test error')
            E       Exception: test error

            test_foo.py:2: Exception

            ================== Test Summary ==================
            Successful Tests:         0
            Failed Tests    :         1
                test_foo.py::test_error: failure\
        "},
        "{contents}"
    );
}

#[test]
fn test_collection_failure() {
    let (contents, _, exit_code) = do_maelstrom_pytest_test(
        &indoc::indoc! {"
            raise Exception('import error')
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: false,
        },
        (100, 100),
    );
    assert_ne!(exit_code, ExitCode::SUCCESS);

    assert!(
        contents.contains(indoc::indoc! {"
            ==================================== ERRORS ====================================
            _________________________ ERROR collecting test_foo.py _________________________
            test_foo.py:1: in <module>
                raise Exception('import error')
            E   Exception: import error
            =========================== short test summary info ============================
            ERROR test_foo.py - Exception: import error
            !!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
        "}),
        "{contents}"
    );
}

#[test]
fn test_listing_all() {
    let contents = do_maelstrom_pytest_test_success(
        &indoc::indoc! {"
            def test_foo():
                pass

            def test_bar():
                pass
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: true,
        },
        (50, 50),
    );

    assert_eq!(
        contents,
        indoc::indoc! {"
            test_foo.py::test_foo
            test_foo.py::test_bar\
        "},
        "{contents}"
    );
}

#[test]
fn test_listing_node_id() {
    let contents = do_maelstrom_pytest_test_success(
        &indoc::indoc! {"
            def test_foo():
                pass

            def test_bar():
                pass
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["node_id.equals(test_foo.py::test_foo)".into()],
                ..Default::default()
            },
            list: true,
        },
        (50, 50),
    );

    assert_eq!(
        contents,
        indoc::indoc! {"
            test_foo.py::test_foo\
        "},
        "{contents}"
    );
}

#[test]
fn test_listing_marker() {
    let contents = do_maelstrom_pytest_test_success(
        &indoc::indoc! {"
            import pytest

            @pytest.mark.baz
            def test_foo():
                pass

            def test_bar():
                pass
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["markers.contains(baz)".into()],
                ..Default::default()
            },
            list: true,
        },
        (50, 50),
    );

    assert_eq!(
        contents,
        indoc::indoc! {"
            test_foo.py::test_foo\
        "},
        "{contents}"
    );
}

#[test]
fn test_ignore() {
    let contents = do_maelstrom_pytest_test_success(
        &indoc::indoc! {"
            import pytest

            @pytest.mark.skip(reason='just because')
            def test_foo():
                pass

            @pytest.mark.skipif(True, reason='just because')
            def test_bar():
                pass

            @pytest.mark.skipif(False, reason='just because')
            def test_baz():
                pass
        "},
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: false,
        },
        (50, 50),
    );

    assert!(
        contents.contains("test_foo.py::test_foo.............IGNORED"),
        "{contents}"
    );
    assert!(
        contents.contains("test_foo.py::test_bar.............IGNORED"),
        "{contents}"
    );
    assert!(
        contents.contains("test_foo.py::test_baz..................OK"),
        "{contents}"
    );

    assert!(
        contents.contains(
            "\
            ================== Test Summary ==================\n\
            Successful Tests:         1\n\
            Failed Tests    :         0\n\
            Ignored Tests   :         2\
        "
        ),
        "{contents}"
    );
}
