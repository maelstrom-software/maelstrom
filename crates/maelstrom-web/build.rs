use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use wasm_bindgen_cli_support::Bindgen;

fn sh<'a>(cmd: impl IntoIterator<Item = &'a str>, dir: impl AsRef<Path>) {
    let path = std::env::var("PATH").unwrap();
    let first_component = path.split(':').next().unwrap();
    if !first_component.starts_with("/nix/store") {
        let maybe_cargo = Path::new(first_component).join("cargo");
        println!("cargo:warning=PATH starts with {first_component} which isn't in /nix/store");
        if maybe_cargo.exists() {
            println!("cargo:warning=A cargo binary exists in {first_component}");
        }
    }

    let cmd: Vec<&str> = cmd.into_iter().collect();
    let status = Command::new(cmd[0])
        .args(&cmd[1..])
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "{cmd:?} failed with status: {status:?}");
}

fn wasm_bindgen(input: &Path, output_dir: &Path) {
    Bindgen::new()
        .web(true)
        .unwrap()
        .input_path(input)
        .nodejs(false)
        .unwrap()
        .browser(false)
        .unwrap()
        .no_modules(false)
        .unwrap()
        .debug(false)
        .demangle(false)
        .keep_lld_exports(false)
        .keep_debug(false)
        .remove_name_section(false)
        .remove_producers_section(false)
        .typescript(true)
        .omit_imports(false)
        .omit_default_module_path(false)
        .split_linked_modules(false)
        .generate(output_dir)
        .unwrap();
}

fn check_for_wasm_opt() -> bool {
    match Command::new("wasm-opt").arg("--help").spawn() {
        Ok(_) => true,
        Err(e) => {
            println!("cargo:warning=Failed to execute wasm-opt: {e}. WASM will not be optimized");
            false
        }
    }
}

fn wasm_opt(input: &Path) {
    if !check_for_wasm_opt() {
        return;
    }

    let mut output = input.to_owned();
    output.set_extension(".opt.wasm");
    sh(
        [
            "wasm-opt",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "-O3",
        ],
        ".",
    );
    fs::rename(&output, input).unwrap();

    let sentinel = input.parent().unwrap().join(".wasm-opt");
    fs::write(sentinel, b"").unwrap();
}

fn create_tar(web_dir: &Path, pkg_dir: &Path, output_file: &Path) {
    let mut builder = tar::Builder::new(fs::File::create(output_file).unwrap());
    for d in [web_dir, pkg_dir] {
        for entry in fs::read_dir(d).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                builder
                    .append_dir_all(entry.file_name(), entry.path())
                    .unwrap();
            } else {
                builder
                    .append_path_with_name(entry.path(), entry.file_name())
                    .unwrap();
            }
        }
    }
    builder.finish().unwrap();
}

fn with_profiles(workspace_root: &Path, body: impl FnOnce() + std::panic::UnwindSafe) {
    let cargo_toml = workspace_root.join("Cargo.toml");
    let cargo_toml_old = workspace_root.join("Cargo.toml.old");
    fs::copy(&cargo_toml, &cargo_toml_old).unwrap();

    let mut f = fs::OpenOptions::new()
        .read(true)
        .append(true)
        .open(&cargo_toml)
        .unwrap();
    f.write_all(
        b"\
        [profile.wasm_dev]\n\
        inherits = \"dev\"\n\
        [profile.wasm_release]\n\
        inherits = \"release\"\n\
    ",
    )
    .unwrap();

    let result = std::panic::catch_unwind(|| {
        body();
    });

    fs::rename(cargo_toml_old, cargo_toml).unwrap();

    result.unwrap();
}

fn build_wasm(target: &str, profile: &str, workspace_root: &Path) {
    with_profiles(workspace_root, || {
        sh(
            [
                "cargo",
                "build",
                "--lib",
                "--target",
                target,
                "--profile",
                profile,
            ],
            ".",
        );
    });
}

fn create_web_tar(profile: &str, build_dir: &Path, workspace_root: &Path) {
    let target = "wasm32-unknown-unknown";
    let pkg_dir = build_dir.join(profile).join("wasm_pkg");

    fs::remove_dir_all(&pkg_dir).ok();
    fs::create_dir_all(&pkg_dir).unwrap();

    build_wasm(target, profile, workspace_root);
    let wasm_file = build_dir
        .join(target)
        .join(profile)
        .join("maelstrom_web.wasm");
    wasm_bindgen(&wasm_file, &pkg_dir);

    if profile == "wasm_release" {
        wasm_opt(&pkg_dir.join("maelstrom_web_bg.wasm"));
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    create_tar(Path::new("www"), &pkg_dir, &out_dir.join("web.tar"));
}

fn main() {
    #[cfg(not(debug_assertions))]
    let profile = "release";

    #[cfg(debug_assertions)]
    let profile = "dev";

    if std::env::var("TARGET").unwrap() != "wasm32-unknown-unknown" {
        let crate_root_cargo_lock = Path::new("Cargo.lock");
        let cargo_lock_existed_at_crate_root = crate_root_cargo_lock.exists();

        let metadata = cargo_metadata::MetadataCommand::new().exec().unwrap();
        create_web_tar(
            &format!("wasm_{profile}"),
            metadata.target_directory.as_std_path(),
            metadata.workspace_root.as_std_path(),
        );

        // this should only be the case when publishing, and at that point we can't be the ones
        // creating the Cargo.lock file
        if !cargo_lock_existed_at_crate_root && crate_root_cargo_lock.exists() {
            fs::remove_file("Cargo.lock").unwrap();
        }
    }
}
