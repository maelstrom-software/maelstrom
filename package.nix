{
  lib,
  stdenv,
  craneLib,
  binaryen,
  pkg-config,
  llvmPackages,
  openssl,
  libiconv,
  version ? null
}:

let
  inherit (craneLib) buildPackage filterCargoSources path;
  inherit (lib) cleanSourceWith optionals;
  inherit (lib.strings) match;

  # Only keeps `.tar` and `.proto` files, plus normal Rust files.
  srcFilter = path: type: (match ".*\.(tar|proto)$" path != null) || (filterCargoSources path type);

  self = buildPackage {
    pname = "maelstrom";
    inherit version;

    src = cleanSourceWith {
      src = path ./.;
      filter = srcFilter;
    };

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
    ];

    buildInputs = [ openssl ] ++ optionals stdenv.isDarwin [ libiconv ];

    # Don't run the unit tests inside Nix build.
    doCheck = false;
  };
in
self
