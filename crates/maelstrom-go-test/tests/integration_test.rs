use indicatif::InMemoryTerm;
use maelstrom_go_test::{cli::ExtraCommandLineOptions, Config, LoggerBuilder};
use maelstrom_test_runner::ui::{self, SimpleUi};
use maelstrom_util::{
    config::common::{CacheSize, ClusterCommunicationStrategy, InlineLimit, LogLevel, Slots},
    fs::Fs,
    process::ExitCode,
    root::RootBuf,
};
use regex::Regex;
use std::path::Path;
use tempfile::tempdir;

fn do_maelstrom_go_test_test(
    temp_dir: &tempfile::TempDir,
    project_dir: &Path,
    extra_options: ExtraCommandLineOptions,
    expected_exit_code: ExitCode,
) -> String {
    let term = InMemoryTerm::new(50, 50);

    let exit_code = maelstrom_test_runner::main_for_test::<maelstrom_go_test::TestRunner>(
        env!("CARGO_BIN_EXE_maelstrom-go-test"),
        ["--client-process"],
        Config {
            parent: maelstrom_test_runner::config::Config {
                client: maelstrom_client::config::Config {
                    broker: None,
                    container_image_depot_root: RootBuf::new(temp_dir.path().join("container")),
                    cache_size: CacheSize::default(),
                    inline_limit: InlineLimit::default(),
                    slots: Slots::default(),
                    accept_invalid_remote_container_tls_certs: true.into(),
                    cluster_communication_strategy: ClusterCommunicationStrategy::Tcp,
                    github_actions_token: None,
                    github_actions_url: None,
                },
                timeout: None,
                log_level: LogLevel::Debug,
                ui: ui::UiKind::Simple,
                repeat: Default::default(),
                stop_after: None,
            },
            go_test: Default::default(),
        },
        extra_options,
        |_| Ok(((), RootBuf::new(project_dir.to_owned()))),
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

    assert!(
        exit_code == expected_exit_code,
        "maelstrom-go-test unexpected exit code: exit_code={:?}, term={}",
        exit_code,
        term.contents()
    );

    term.contents()
}

#[test]
fn many_different_tests_success() {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");

    fs.create_dir(&project_dir).unwrap();
    fs.create_dir(project_dir.join("pkg1")).unwrap();
    fs.create_dir(project_dir.join("pkg2")).unwrap();
    fs.create_dir(project_dir.join("pkg3")).unwrap();

    fs.write(project_dir.join("go.mod"), "module maelstrom-software.com")
        .unwrap();

    let source_contents = indoc::indoc! {"
        package foo;

        import \"fmt\"
        import \"testing\"

        func TestA(t *testing.T) {}
        func ExampleB() {
            fmt.Println(\"foo\")
            // Output: foo
        }
        func TestC(t *testing.T) {
            t.Skip(\"hi\")
        }
    "};
    fs.write(project_dir.join("pkg1/foo_test.go"), source_contents)
        .unwrap();

    let source_contents = indoc::indoc! {"
        package bar;
        import \"testing\"

        func TestA(t *testing.T) {}
        func FuzzB(f *testing.F) {
            f.Add(1)
            f.Fuzz(func(t *testing.T, i int) {
                if i > 100 {
                    t.Fatalf(\"%d > 100\", i)
                }
            })
        }
    "};
    fs.write(project_dir.join("pkg2/bar_test.go"), source_contents)
        .unwrap();

    let source_contents = indoc::indoc! {"
        package empty;
    "};
    fs.write(project_dir.join("pkg3/empty.go"), source_contents)
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
        ExitCode::SUCCESS,
    );
    let tests = [
        "maelstrom-software.com/pkg1 TestA......OK",
        "maelstrom-software.com/pkg1 ExampleB...OK",
        "maelstrom-software.com/pkg2 TestA......OK",
        "maelstrom-software.com/pkg2 FuzzB......OK",
        "maelstrom-software.com/pkg1 TestC.IGNORED",
    ];
    for t in tests {
        assert!(contents.contains(t), "{t} not in {contents}");
    }
    assert!(
        Regex::new(
            "(?ms)^\
            .*\n\
            ================== Test Summary ==================\n\
            Successful Tests:         4\n\
            Failed Tests    :         0\n\
            Ignored Tests   :         1\n\
            \\s\\s\\s\\smaelstrom-software.com/pkg1 TestC: ignored\
            $"
        )
        .unwrap()
        .is_match(&contents),
        "{contents}"
    );
}

#[test]
fn single_test_failure() {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");

    fs.create_dir(&project_dir).unwrap();
    fs.create_dir(project_dir.join("pkg1")).unwrap();

    fs.write(project_dir.join("go.mod"), "module maelstrom-software.com")
        .unwrap();

    let source_contents = indoc::indoc! {"
        package foo;

        import \"fmt\"
        import \"testing\"

        func TestA(t *testing.T) {
            fmt.Println(\"test output\")
            t.Fatal(\"test failure\")
        }
    "};
    fs.write(project_dir.join("pkg1/foo_test.go"), source_contents)
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
        ExitCode::from(1),
    );

    assert!(
        Regex::new(
            "(?ms)^\
            maelstrom-software.com/pkg1 TestA....FAIL   [\\d\\.]+s\n\
            test output\n\
            \\s\\s\\s\\sfoo_test.go:8: test failure\n\
            \n\
            ================== Test Summary ==================\n\
            Successful Tests:         0\n\
            Failed Tests    :         1\n\
            \\s\\s\\s\\smaelstrom-software.com/pkg1 TestA: failure\
            $"
        )
        .unwrap()
        .is_match(&contents),
        "{contents}"
    );
}

#[test]
fn single_fuzz_failure() {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");

    fs.create_dir(&project_dir).unwrap();
    fs.create_dir(project_dir.join("pkg1")).unwrap();

    fs.write(project_dir.join("go.mod"), "module maelstrom-software.com")
        .unwrap();

    let source_contents = indoc::indoc! {"
        package foo;

        import \"testing\"

        func FuzzB(f *testing.F) {
            f.Add(1)
            f.Add(200)
            f.Fuzz(func(t *testing.T, i int) {
                if i > 100 {
                    t.Fatalf(\"%d > 100\", i)
                }
            })
        }
    "};
    fs.write(project_dir.join("pkg1/foo_test.go"), source_contents)
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
        ExitCode::from(1),
    );

    assert!(
        Regex::new(
            "(?ms)^\
            maelstrom-software.com/pkg1 FuzzB....FAIL   [\\d\\.]+s\n\
            === RUN   FuzzB\n\
            === RUN   FuzzB/seed\\#0\n\
            === RUN   FuzzB/seed\\#1\n\
            \\s\\s\\s\\sfoo_test.go:10: 200 > 100\n\
            --- FAIL: FuzzB \\([\\d\\.]+s\\)\n\
            \\s\\s\\s\\s--- PASS: FuzzB/seed\\#0 \\([\\d\\.]+s\\)\n\
            \\s\\s\\s\\s--- FAIL: FuzzB/seed\\#1 \\([\\d\\.]+s\\)\n\
            \n\
            ================== Test Summary ==================\n\
            Successful Tests:         0\n\
            Failed Tests    :         1\n\
            \\s\\s\\s\\smaelstrom-software.com/pkg1 FuzzB: failure\
            $"
        )
        .unwrap()
        .is_match(&contents),
        "{contents}"
    );
}

#[test]
fn single_example_failure() {
    let fs = Fs::new();
    let temp_dir = tempdir().unwrap();

    let project_dir = temp_dir.path().join("project");

    fs.create_dir(&project_dir).unwrap();
    fs.create_dir(project_dir.join("pkg1")).unwrap();

    fs.write(project_dir.join("go.mod"), "module maelstrom-software.com")
        .unwrap();

    let source_contents = indoc::indoc! {"
        package foo;

        import \"fmt\"

        func ExampleB() {
            fmt.Println(\"foo\")
            // Output: bar
        }
    "};
    fs.write(project_dir.join("pkg1/foo_test.go"), source_contents)
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
        ExitCode::from(1),
    );

    assert!(
        Regex::new(
            "(?ms)^\
            maelstrom-software.com/pkg1 ExampleB.FAIL   [\\d\\.]+s\n\
            got:\n\
            foo\n\
            want:\n\
            bar\n\
            \n\
            ================== Test Summary ==================\n\
            Successful Tests:         0\n\
            Failed Tests    :         1\n\
            \\s\\s\\s\\smaelstrom-software.com/pkg1 ExampleB: failure\
            $"
        )
        .unwrap()
        .is_match(&contents),
        "{contents}"
    );
}
