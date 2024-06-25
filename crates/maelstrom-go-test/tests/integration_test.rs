use indicatif::InMemoryTerm;
use maelstrom_client::{ClientBgProcess, ProjectDir};
use maelstrom_go_test::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
};
use std::path::PathBuf;
use tempfile::tempdir;

fn spawn_bg_proc() -> ClientBgProcess {
    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_maelstrom-go-test"));
    ClientBgProcess::new_from_bin(&bin_path, &["--client-bg-proc"]).unwrap()
}

fn do_maelstrom_go_test_test(
    source_contents: &str,
    extra_options: ExtraCommandLineOptions,
) -> String {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");
    fs.create_dir(&project_dir).unwrap();
    fs.write(project_dir.join("foo_test.go"), source_contents)
        .unwrap();

    fs.write(project_dir.join("foo.go"), "package foo").unwrap();
    fs.write(project_dir.join("go.mod"), "module foo").unwrap();

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
    let term = InMemoryTerm::new(50, 50);

    let log = maelstrom_util::log::test_logger();
    let logger = Logger::GivenLogger(log.clone());

    let mut stderr = vec![];
    let bg_proc = spawn_bg_proc();
    let exit_code = maelstrom_go_test::main(
        config,
        extra_options,
        &Root::<ProjectDir>::new(&project_dir),
        bg_proc,
        logger,
        false,
        false,
        term.clone(),
        &mut stderr,
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
    let contents = do_maelstrom_go_test_test(
        &indoc::indoc! {"
        package foo;
        import \"testing\"

        func TestA(t *testing.T) {}
    "},
        ExtraCommandLineOptions {
            include: vec!["all".into()],
            exclude: vec![],
            list: false,
            client_bg_proc: false,
        },
    );
    assert!(
        contents.contains("foo TestA..............................OK"),
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
