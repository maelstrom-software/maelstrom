use std::path::Path;
use std::process::Command;

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
    println!("cargo:rerun-if-changed=../meticulous-web/build.sh");
    println!("cargo:rerun-if-changed=../meticulous-web/Cargo.toml");
    println!("cargo:rerun-if-changed=../meticulous-web/src/");
    println!("cargo:rerun-if-changed=../meticulous-web/www/");
    println!("cargo:rerun-if-changed=../meticulous-plot/src/");
    println!("cargo:rerun-if-changed=../meticulous-base/src/");

    sh(["crates/meticulous-web/build.sh"], "../..");
}
