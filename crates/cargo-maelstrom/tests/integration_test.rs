use cargo_maelstrom::{
    cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions},
    cli::{ExtraCommandLineOptions, ListOptions, TestMetadataOptions},
    config::Config,
    Logger,
};
use indicatif::InMemoryTerm;
use maelstrom_client::ClientBgProcess;
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
};
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

    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_client-bg-proc"));
    ClientBgProcess::new_from_bin(&bin_path).unwrap()
}

fn cmd(args: &[&str], current_dir: &Path) {
    let mut cmd = Command::new(args[0]);
    cmd.args(&args[1..])
        .current_dir(current_dir)
        .stderr(Stdio::null())
        .stdout(Stdio::null());
    assert!(cmd.status().unwrap().success(), "{cmd:?} failed");
}

#[test]
fn empty_cargo_project() {
    let bg_proc = spawn_bg_proc();

    let _fs = Fs::new();
    let temp_dir = tempdir().unwrap();
    cmd(&["cargo", "new", "project"], temp_dir.path());

    let config = Config {
        broker: None,
        log_level: LogLevel::Debug,
        quiet: false.into(),
        timeout: None,
        cache_size: CacheSize::default(),
        inline_limit: InlineLimit::default(),
        slots: Slots::default(),
        cargo_feature_selection_options: FeatureSelectionOptions::default(),
        cargo_compilation_options: CompilationOptions::default(),
        cargo_manifest_options: ManifestOptions::default(),
    };
    let extra_options = ExtraCommandLineOptions {
        include: vec![],
        exclude: vec![],
        list: ListOptions {
            tests: false,
            binaries: false,
            packages: false,
        },
        test_metadata: TestMetadataOptions { init: false },
    };
    let term = InMemoryTerm::new(50, 50);

    let log = maelstrom_util::log::test_logger();
    let logger = Logger::GivenLogger(log.clone());

    std::env::set_current_dir(temp_dir.path().join("project")).unwrap();
    cargo_maelstrom::main(
        config,
        extra_options,
        bg_proc,
        logger,
        false,
        false,
        term.clone(),
    )
    .unwrap();

    assert_eq!(
        term.contents(),
        "\n\
        ================== Test Summary ==================\n\
        Successful Tests:         0\n\
        Failed Tests    :         0\
    "
    );
}
