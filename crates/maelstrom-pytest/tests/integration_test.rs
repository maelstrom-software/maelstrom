use anyhow::{bail, Result};
use indicatif::InMemoryTerm;
use maelstrom_client::{ClientBgProcess, ProjectDir};
use maelstrom_container::local_registry;
use maelstrom_pytest::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    root::{Root, RootBuf},
};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempfile::tempdir;

fn spawn_bg_proc() -> ClientBgProcess {
    // XXX cargo-maelstrom doesn't add shared-library dependencies for additional binaries.
    //
    // To make us have the same dependencies as the client-process, call into the client-process
    // code in some code-path which won't execute but the compiler won't optimize out.
    if std::env::args().next().unwrap() == "not_going_to_happen" {
        maelstrom_client::bg_proc_main().unwrap();
    }

    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_pytest-client-bg-proc"));
    ClientBgProcess::new_from_bin(&bin_path).unwrap()
}

fn sh(script: &str) -> Result<()> {
    let mut cmd = Command::new("/bin/sh");
    cmd.args(["-c", script])
        .stderr(Stdio::null())
        .stdout(Stdio::null());
    let mut child = cmd.spawn()?;

    let exit_status = child.wait()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!("sh failed")
    }
}

fn maybe_install_pytest() {
    if sh("python -c 'import pytest'").is_err() {
        sh("pip install pytest==8.1.1").unwrap()
    }
}

fn do_maelstrom_pytest_test(source_contents: &str) -> String {
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
            quiet: false.into(),
            container_image_depot_root,
            timeout: None,
            cache_size: CacheSize::default(),
            inline_limit: InlineLimit::default(),
            slots: Slots::default(),
            accept_invalid_remote_container_tls_certs: true.into(),
        },
    };
    let extra_options = ExtraCommandLineOptions {
        include: vec!["all".into()],
        exclude: vec![],
        list: false,
    };
    let term = InMemoryTerm::new(50, 50);

    let logger = Logger::GivenLogger(log.clone());

    let bg_proc = spawn_bg_proc();
    maelstrom_pytest::main(
        config,
        extra_options,
        &Root::<ProjectDir>::new(&project_dir),
        bg_proc,
        logger,
        false,
        false,
        term.clone(),
    )
    .unwrap();

    term.contents()
}

#[test]
fn test_simple() {
    let contents = do_maelstrom_pytest_test(&indoc::indoc! {"
        def test_noop():
            pass
    "});
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
