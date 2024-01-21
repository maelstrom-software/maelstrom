use std::{
    path::{Path, PathBuf},
    process::Command,
};

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
    println!("cargo:rerun-if-changed=build.sh");

    #[cfg(not(debug_assertions))]
    let profile = "release";

    #[cfg(debug_assertions)]
    let profile = "dev";

    let mut build_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    // should be some path like ../target/debug/build/maelstrom-web-<hash>/out
    for _ in 0..4 {
        build_dir.pop();
    }

    if std::env::var("TARGET").unwrap() != "wasm32-unknown-unknown" {
        sh(
            [
                "./build.sh",
                &format!("wasm_{profile}"),
                build_dir.to_str().unwrap(),
            ],
            ".",
        );
    }
}
