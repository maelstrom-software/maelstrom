{
  craneLib,
  gh,
  mdbook,
  bat,
  cargo-audit,
  cargo-binstall,
  cargo-edit,
  cargo-nextest,
  cargo-watch,
  protobuf3_20,
  ripgrep,
  rust-analyzer,
  stgit,
  maelstrom,
}:

craneLib.devShell {
  inputsFrom = [ maelstrom ];

  # Extra inputs (only used for interactive development)
  # can be added here; cargo and rustc are provided by default.
  packages = [
    gh
    mdbook
    bat
    cargo-audit
    cargo-binstall
    cargo-edit
    cargo-nextest
    cargo-watch
    protobuf3_20
    ripgrep
    rust-analyzer
    stgit
  ];

  env.CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER = "lld";
}
