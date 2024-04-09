{
  description = "Maelstrom is an extremely fast Rust test runner built on top of a general-purpose clustered job runner.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
      flake-utils,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        rustToolchain = pkgs.rust-bin.stable."1.75.0".default.override {
          extensions = [ "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };
        craneLib = ((crane.mkLib pkgs).overrideToolchain rustToolchain);
        all = craneLib.buildPackage {
          # NOTE: we need to force lld otherwise rust-lld is not found for wasm32 target
          CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER = "lld";

          pname = "all";
          src =
            let
              # Only keeps markdown files
              tarFilter = path: _type: builtins.match ".*tar$" path != null;
              tarOrCargo = path: type: (tarFilter path type) || (craneLib.filterCargoSources path type);
            in
            nixpkgs.lib.cleanSourceWith {
              src = craneLib.path ./.;
              filter = tarOrCargo;
            };
          strictDeps = true;

          nativeBuildInputs = [
            pkgs.binaryen
            pkgs.pkg-config
            pkgs.llvmPackages.bintools
          ];

          buildInputs =
            [
              pkgs.openssl
              # Add additional build inputs here
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              # Additional darwin specific inputs can be set here
              pkgs.libiconv
            ];

          doCheck = false;

          # Additional environment variables can be set directly
          # MY_CUSTOM_VAR = "some value";
        };
      in
      {
        packages.default = all;

        devShells.default = craneLib.devShell {
          # Automatically inherit any build inputs from `my-crate`
          inputsFrom = [ all ];

          # Extra inputs (only used for interactive development)
          # can be added here; cargo and rustc are provided by default.
          packages = [
            pkgs.gh
            pkgs.mdbook
            pkgs.bat
            pkgs.cargo-audit
            pkgs.cargo-binstall
            pkgs.cargo-edit
            pkgs.cargo-nextest
            pkgs.cargo-watch
            pkgs.protobuf3_20
            pkgs.ripgrep
            pkgs.rust-analyzer
            pkgs.stgit
          ];

          CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER = "lld";
        };
      }
    );
}
