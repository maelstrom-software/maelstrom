use cargo_maelstrom::{
    cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions},
    cli::{ExtraCommandLineOptions, ListOptions},
    config::Config,
    LoggerBuilder,
};
use indicatif::InMemoryTerm;
use maelstrom_base::Timeout;
use maelstrom_client::{Client, ClientBgProcess};
use maelstrom_test_runner::{
    log::LogDestination,
    run_app_with_ui_multithreaded,
    ui::{self, Ui as _},
    TestRunner as _,
};
use maelstrom_util::{
    config::common::{ArtifactTransferStrategy, CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    root::RootBuf,
};
use regex::Regex;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};
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

    let config = Config {
        parent: maelstrom_test_runner::config::Config {
            broker: None,
            log_level: LogLevel::Debug,
            container_image_depot_root: RootBuf::new(PathBuf::from(".cache/maelstrom/container")),
            timeout: None,
            cache_size: CacheSize::default(),
            inline_limit: InlineLimit::default(),
            slots: Slots::default(),
            accept_invalid_remote_container_tls_certs: true.into(),
            ui: ui::UiKind::Simple,
            repeat: Default::default(),
            stop_after: None,
            artifact_transfer_strategy: ArtifactTransferStrategy::TcpUpload,
        },
        cargo_feature_selection_options: FeatureSelectionOptions::default(),
        cargo_compilation_options: CompilationOptions::default(),
        cargo_manifest_options: ManifestOptions {
            manifest_path: Some(temp_dir.path().join("project/Cargo.toml")),
            ..Default::default()
        },
        extra_test_binary_args: vec![],
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
    let logger = LoggerBuilder::GivenLogger(log.clone());

    let bg_proc = spawn_bg_proc();

    let ui = ui::SimpleUi::new(false, false, term.clone());

    (|| {
        let mut ui = Some(ui);
        let start_ui = || {
            let log_destination = LogDestination::default();
            let log = logger.build(log_destination.clone());
            ui.take().unwrap().start_ui_thread(log_destination, log)
        };
        if let Some(result) =
            cargo_maelstrom::TestRunner::execute_alternative_main(&config, &extra_options, start_ui)
        {
            return result;
        }

        let (directories, metadata) =
            cargo_maelstrom::TestRunner::get_directories_and_metadata(&config)?;

        Fs.create_dir_all(&directories.state)?;
        Fs.create_dir_all(&directories.cache)?;

        let log_destination = LogDestination::default();
        let log = logger.build(log_destination.clone());

        let (parent_config, collector_config) = cargo_maelstrom::TestRunner::split_config(config);
        let client = Client::new(
            bg_proc,
            parent_config.broker,
            &directories.project,
            &directories.state,
            parent_config.container_image_depot_root,
            &directories.cache,
            parent_config.cache_size,
            parent_config.inline_limit,
            parent_config.slots,
            parent_config.accept_invalid_remote_container_tls_certs,
            parent_config.artifact_transfer_strategy,
            log.clone(),
        )?;

        let list_tests = cargo_maelstrom::TestRunner::is_list_tests(&extra_options).into();
        let parent_extra_options =
            cargo_maelstrom::TestRunner::extra_options_into_parent(extra_options);
        let template_vars =
            cargo_maelstrom::TestRunner::get_template_vars(&collector_config, &directories)?;

        run_app_with_ui_multithreaded(
            log_destination,
            parent_config.timeout.map(Timeout::new),
            ui.unwrap(),
            &cargo_maelstrom::TestRunner::get_test_collector(
                &client,
                &directories,
                &log,
                metadata,
            )?,
            parent_extra_options.include,
            parent_extra_options.exclude,
            list_tests,
            parent_config.repeat,
            parent_config.stop_after,
            parent_extra_options.watch,
            false,
            &directories.project,
            &directories.state,
            cargo_maelstrom::TestRunner::get_watch_exclude_paths(&directories),
            collector_config,
            log,
            &client,
            cargo_maelstrom::TestRunner::TEST_METADATA_FILE_NAME,
            cargo_maelstrom::TestRunner::DEFAULT_TEST_METADATA_FILE_CONTENTS,
            template_vars,
        )
    })()
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
