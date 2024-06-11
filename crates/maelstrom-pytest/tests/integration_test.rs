use indicatif::InMemoryTerm;
use maelstrom_client::ClientBgProcess;
use maelstrom_pytest::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_util::{
    config::common::{CacheSize, InlineLimit, LogLevel, Slots},
    root::RootBuf,
};
use std::path::PathBuf;

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

#[allow(dead_code)]
fn do_cargo_maelstrom_test(_source_contents: &str) -> String {
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
        },
    };
    let extra_options = ExtraCommandLineOptions {
        include: vec!["all".into()],
        exclude: vec![],
        list: false,
    };
    let term = InMemoryTerm::new(50, 50);

    let log = maelstrom_util::log::test_logger();
    let logger = Logger::GivenLogger(log.clone());

    let bg_proc = spawn_bg_proc();
    maelstrom_pytest::main(
        config,
        extra_options,
        bg_proc,
        logger,
        false,
        false,
        term.clone(),
    )
    .unwrap();

    term.contents()
}
