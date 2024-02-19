use compiler_cli_args::compiler_cli_args;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write as _;
use std::path::Path;
use std::process::Command;

fn key_value(args: &[&'static str]) -> HashMap<&'static str, &'static str> {
    args.iter()
        .copied()
        .filter_map(|a| {
            a.contains('=').then(|| {
                let mut split = a.split('=');
                (split.next().unwrap(), split.next().unwrap())
            })
        })
        .collect()
}

const COMPILER_CLI_ARGS: &[&str] = &compiler_cli_args!();

fn main() {
    #[cfg(not(debug_assertions))]
    let profile = "release";

    #[cfg(debug_assertions)]
    let profile = "dev";

    println!("cargo:rerun-if-changed=build.rs");
    let key_values = key_value(COMPILER_CLI_ARGS);
    let tempdir = tempfile::tempdir().unwrap();

    let src_file_path = tempdir.path().join("main.rs");
    let mut src_file = File::create(&src_file_path).unwrap();
    write!(
        src_file,
        "fn main() {{ maelstrom_client_process::main().unwrap() }}"
    )
    .unwrap();

    let output = Path::new(&std::env::var_os("OUT_DIR").unwrap()).join("maelstrom_client_process");
    let mut cmd = Command::new("rustc");
    cmd.arg("--edition=2021")
        .arg(src_file_path.as_os_str())
        .arg("-o")
        .arg(output.as_os_str())
        .arg("-L")
        .arg(&format!("dependency={}", key_values["dependency"]))
        .arg("--extern")
        .arg(&format!(
            "maelstrom_client_process={}",
            key_values["maelstrom_client_process"]
        ));

    if profile == "release" {
        cmd.arg("-C").arg("opt-level=3");
    }

    let res = cmd.output().unwrap();
    if !res.status.success() {
        panic!("rustc failure: {}", String::from_utf8_lossy(&res.stderr));
    }

    println!("cargo:rustc-env=CLIENT_EXE={}", output.display());
}
