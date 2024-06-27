use anyhow::Result;
use cargo_maelstrom::cli::ExtraCommandLineOptions;
use maelstrom_util::process::ExitCode;
use std::env;

pub fn main() -> Result<ExitCode> {
    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    maelstrom_test_runner::main(
        clap::command!(),
        "maelstrom/cargo-maelstrom",
        "CARGO_MAELSTROM",
        args,
        |extra_options: &ExtraCommandLineOptions| {
            extra_options.list.tests || extra_options.list.binaries || extra_options.list.packages
        },
        cargo_maelstrom::main,
    )
}
