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
    println!("cargo:rerun-if-changed=../web/src/");
    println!("cargo:rerun-if-changed=../web/www/");

    sh(["web/build.sh"], "..");
}
