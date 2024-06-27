use cargo_maelstrom::{
    cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions},
    cli::{ExtraCommandLineOptions, ListOptions},
    config::Config,
    Logger,
};
use indicatif::InMemoryTerm;
use maelstrom_client::ClientBgProcess;
use maelstrom_test_runner::ui;
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    root::RootBuf,
};
use regex::Regex;
use std::path::Path;
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

    let config = Config {
        parent: maelstrom_test_runner::config::Config {
            broker: None,
            log_level: LogLevel::Debug,
            quiet: false.into(),
            container_image_depot_root: RootBuf::new(PathBuf::from(".cache/maelstrom/container")),
            timeout: None,
            cache_size: CacheSize::default(),
            inline_limit: InlineLimit::default(),
            slots: Slots::default(),
            accept_invalid_remote_container_tls_certs: true.into(),
            ui: ui::UiKind::Simple,
        },
        cargo_feature_selection_options: FeatureSelectionOptions::default(),
        cargo_compilation_options: CompilationOptions::default(),
        cargo_manifest_options: ManifestOptions {
            manifest_path: Some(temp_dir.path().join("project/Cargo.toml")),
            ..Default::default()
        },
    };
    let extra_options = ExtraCommandLineOptions {
        parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
            include: vec!["all".into()],
            ..Default::default()
        },
        list: ListOptions {
            tests: false,
            binaries: false,
            packages: false,
        },
    };
    let term = InMemoryTerm::new(50, 50);

    let log = maelstrom_util::log::test_logger();
    let logger = Logger::GivenLogger(log.clone());

    let bg_proc = spawn_bg_proc();

    let ui = ui::SimpleUi::new(false, false, false.into(), term.clone());
    cargo_maelstrom::main(config, extra_options, bg_proc, logger, false, ui).unwrap();

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
