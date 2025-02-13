use cargo_maelstrom::{
    cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions},
    cli::{ExtraCommandLineOptions, ListOptions},
    config::{CargoConfig, Config},
    LoggerBuilder,
};
use indicatif::InMemoryTerm;
use maelstrom_client::ClientBgProcess;
use maelstrom_test_runner::{
    ui::{SimpleUi, UiKind},
    TestRunner as _,
};
use maelstrom_util::{
    config::common::{ArtifactTransferStrategy, CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    root::RootBuf,
};
use regex::Regex;
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tempfile::tempdir;

fn spawn_bg_proc() -> ClientBgProcess {
    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_cargo-maelstrom"));
    ClientBgProcess::new_from_bin(&bin_path, &["--client-bg-proc"]).unwrap()
}

fn cmd(args: &[&str], current_dir: &Path) {
    let mut cmd = Command::new(args[0]);
    cmd.args(&args[1..])
        .current_dir(current_dir)
        .stderr(Stdio::null())
        .stdout(Stdio::null());
    assert!(cmd.status().unwrap().success(), "{cmd:?} failed");
}

fn do_cargo_maelstrom_test(source_contents: &str) -> String {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();
    cmd(&["cargo", "new", "project"], temp_dir.path());
    fs.write(temp_dir.path().join("project/src/lib.rs"), source_contents)
        .unwrap();

    let term = InMemoryTerm::new(50, 50);

    maelstrom_test_runner::main_for_test::<cargo_maelstrom::TestRunner>(
        spawn_bg_proc(),
        Config {
            parent: maelstrom_test_runner::config::Config {
                broker: None,
                log_level: LogLevel::Debug,
                container_image_depot_root: RootBuf::new(PathBuf::from(
                    ".cache/maelstrom/container",
                )),
                timeout: None,
                cache_size: CacheSize::default(),
                inline_limit: InlineLimit::default(),
                slots: Slots::default(),
                accept_invalid_remote_container_tls_certs: true.into(),
                ui: UiKind::Simple,
                repeat: Default::default(),
                stop_after: None,
                artifact_transfer_strategy: ArtifactTransferStrategy::TcpUpload,
            },
            cargo: CargoConfig {
                feature_selection_options: FeatureSelectionOptions::default(),
                compilation_options: CompilationOptions::default(),
                manifest_options: ManifestOptions {
                    manifest_path: Some(temp_dir.path().join("project/Cargo.toml")),
                    ..Default::default()
                },
                extra_test_binary_args: vec![],
            },
        },
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: ListOptions {
                tests: false,
                binaries: false,
                packages: false,
            },
        },
        cargo_maelstrom::TestRunner::get_metadata_and_project_directory,
        LoggerBuilder::GivenLogger(maelstrom_util::log::test_logger()),
        |_, is_listing, stdout_tty| {
            Ok(Box::new(SimpleUi::new(
                is_listing,
                stdout_tty,
                term.clone(),
            )))
        },
    )
    .unwrap();

    term.contents()
}

#[test]
fn empty_cargo_project() {
    let contents = do_cargo_maelstrom_test("");
    let expected = "\n\
        ================== Test Summary ==================\n\
        Successful Tests:         0\n\
        Failed Tests    :         0\
    ";
    assert_eq!(contents, expected);
}

#[test]
fn few_tests() {
    let contents = do_cargo_maelstrom_test(
        "
        #[test]
        fn foo() {}

        #[test]
        fn bar() {}
        ",
    );
    assert!(
        contents.contains("project foo............................OK"),
        "{contents}"
    );
    assert!(
        contents.contains("project bar............................OK"),
        "{contents}"
    );
    assert!(
        contents.ends_with(
            "\
            ================== Test Summary ==================\n\
            Successful Tests:         2\n\
            Failed Tests    :         0\
        "
        ),
        "{contents}"
    );
}

#[test]
fn failed_test() {
    let contents = do_cargo_maelstrom_test(
        "
        #[test]
        fn foo() {
            println!(\"test output\");
            assert_eq!(1, 2);
        }
        ",
    );
    assert!(
        Regex::new(
            "(?ms)^\
            project foo..........................FAIL   [\\d\\.]+s\n\
            test output\n\
            stderr: thread 'foo' panicked at src/lib.rs:\\d+:\\d+:\n\
            stderr: assertion `left == right` failed\n\
            stderr:   left: 1\n\
            stderr:  right: 2\n\
            stderr: note: run with `RUST_BACKTRACE=1` environm\n\
            ent variable to display a backtrace\n\
            \n\
            ================== Test Summary ==================\n\
                Successful Tests:         0\n\
                Failed Tests    :         1\n\
                \\s\\s\\s\\sproject foo: failure\
            $"
        )
        .unwrap()
        .is_match(&contents),
        "{contents}"
    );
}
