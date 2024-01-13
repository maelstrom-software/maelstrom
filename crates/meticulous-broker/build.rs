use std::{path::Path, process::Command};

fn sh<'a>(cmd: impl IntoIterator<Item = &'a str>, dir: impl AsRef<Path>) {
    let cmd: Vec<&str> = cmd.into_iter().collect();
    let status = Command::new(cmd[0])
        .args(&cmd[1..])
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "{cmd:?} failed with status: {status:?}");
}

fn main() {
    println!("cargo:rerun-if-changed=../maelstrom-web/build.sh");
    println!("cargo:rerun-if-changed=../maelstrom-web/Cargo.toml");
    println!("cargo:rerun-if-changed=../maelstrom-web/src/");
    println!("cargo:rerun-if-changed=../maelstrom-web/www/");
    println!("cargo:rerun-if-changed=../maelstrom-plot/src/");
    println!("cargo:rerun-if-changed=../maelstrom-base/src/");

    #[cfg(not(debug_assertions))]
    let profile = "release";

    #[cfg(debug_assertions)]
    let profile = "dev";

    sh(
        ["crates/maelstrom-web/build.sh", &format!("wasm_{profile}")],
        "../..",
    );
}
