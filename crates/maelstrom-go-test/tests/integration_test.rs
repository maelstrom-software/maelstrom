use indicatif::InMemoryTerm;
use maelstrom_client::{ClientBgProcess, ProjectDir};
use maelstrom_go_test::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_test_runner::ui;
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn spawn_bg_proc() -> ClientBgProcess {
    // XXX cargo-maelstrom doesn't add shared-library dependencies for additional binaries.
    //
    // To make us have the same dependencies as the client-process, call into the client-process
    // code in some code-path which won't execute but the compiler won't optimize out.
    if std::env::args().next().unwrap() == "not_going_to_happen" {
        maelstrom_client::bg_proc_main().unwrap();
    }

    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_maelstrom-go-test"));
    ClientBgProcess::new_from_bin(&bin_path, &["--client-bg-proc"]).unwrap()
}

fn do_maelstrom_go_test_test(
    temp_dir: &tempfile::TempDir,
    project_dir: &Path,
    extra_options: ExtraCommandLineOptions,
) -> String {
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
            ui: ui::UiKind::Simple,
            repeat: Default::default(),
        },
    };
    let term = InMemoryTerm::new(50, 50);

    let log = maelstrom_util::log::test_logger();
    let logger = Logger::GivenLogger(log.clone());

    let ui = ui::SimpleUi::new(false, false, false.into(), term.clone());
    let mut stderr = vec![];
    let bg_proc = spawn_bg_proc();
    let exit_code = maelstrom_go_test::main_with_stderr_and_project_dir(
        config,
        extra_options,
        bg_proc,
        logger,
        false,
        ui,
        &mut stderr,
        &Root::<ProjectDir>::new(&project_dir),
    )
    .unwrap();

    assert!(
        exit_code == ExitCode::SUCCESS,
        "maelstrom-go-test failed: {}: {}",
        String::from_utf8(stderr).unwrap(),
        term.contents()
    );

    term.contents()
}

#[test]
fn test_simple_success() {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");

    fs.create_dir(&project_dir).unwrap();
    fs.create_dir(project_dir.join("foo")).unwrap();
    fs.create_dir(project_dir.join("bar")).unwrap();

    fs.write(project_dir.join("go.mod"), "module baz").unwrap();

    let source_contents = indoc::indoc! {"
        package foo;
        import \"testing\"

        func TestA(t *testing.T) {}
    "};
    fs.write(project_dir.join("foo/foo_test.go"), source_contents)
        .unwrap();

    let source_contents = indoc::indoc! {"
        package bar;
        import \"testing\"

        func TestA(t *testing.T) {}
    "};
    fs.write(project_dir.join("bar/bar_test.go"), source_contents)
        .unwrap();

    let contents = do_maelstrom_go_test_test(
        &temp_dir,
        &project_dir,
        ExtraCommandLineOptions {
            parent: maelstrom_test_runner::config::ExtraCommandLineOptions {
                include: vec!["all".into()],
                ..Default::default()
            },
            list: Default::default(),
        },
    );
    assert!(
        contents.contains("baz/foo TestA..........................OK"),
        "{contents}"
    );
    assert!(
        contents.contains("baz/bar TestA..........................OK"),
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
