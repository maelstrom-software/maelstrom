{
  lib,
  stdenv,
  craneLib,
  binaryen,
  pkg-config,
  protobuf,
  llvmPackages,
  openssl,
  libiconv,
}:

let
  inherit (craneLib) buildPackage filterCargoSources path;
  inherit (lib) cleanSourceWith optionals;
  inherit (lib.strings) match;

  # Only keeps `.tar` and `.proto` files
  allowList = path: _type: match ".*\.(tar|proto)$" path != null;
  srcFilter = path: type: (allowList path type) || (filterCargoSources path type);

  src = cleanSourceWith {
    src = path ./.;
    filter = srcFilter;
  };

  self = buildPackage {
    # NOTE: we need to force lld otherwise rust-lld is not found for wasm32 target
    env.CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER = "lld";

    pname = "all";
    inherit src;

    strictDeps = true;

    # maelstrom-web has a WASM build step that shells out to `cargo`, and needs to be informed about
    # being inside the Nix build sandbox.
    postUnpack = ''
      mkdir source/crates/maelstrom-web/.cargo
      ln -s ${self.cargoVendorDir}/config.toml source/crates/maelstrom-web/.cargo/config.toml
    '';

    nativeBuildInputs = [
      binaryen
      pkg-config
      llvmPackages.bintools
      protobuf
    ];

    buildInputs = [ openssl ] ++ optionals stdenv.isDarwin [ libiconv ];

    doCheck = false;
  };
in
self
