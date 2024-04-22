{
  description = "Maelstrom is an extremely fast Rust test runner built on top of a general-purpose clustered job runner.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    systems.url = "github:nix-systems/default";

    crane.url = "github:ipetkov/crane";
    crane.inputs.nixpkgs.follows = "nixpkgs";

    flake-utils.url = "github:numtide/flake-utils";
    flake-utils.inputs.systems.follows = "systems";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.inputs.flake-utils.follows = "flake-utils";
  };

  outputs =
    inputs:
    let
      inherit (inputs) self nixpkgs rust-overlay;
      inherit (inputs.crane) mkLib;
      inherit (inputs.flake-utils.lib) eachDefaultSystem;
      inherit (inputs.nixpkgs.lib) importTOML;

      cargoToml = importTOML ./Cargo.toml;

      inherit (cargoToml.workspace.package) rust-version version;
    in
    eachDefaultSystem (
      system:
      let
        pkgs = self.legacyPackages.${system};

        # Use the Rust toolchain from Cargo.toml
        rustToolchain = pkgs.rust-bin.stable.${rust-version}.default.override {
          extensions = [ "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        craneLib = ((mkLib pkgs).overrideToolchain rustToolchain);
      in
      {
        # Import nixpkgs once
        legacyPackages = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        packages.default = pkgs.callPackage ./package.nix { inherit craneLib version; };

        devShells.default = pkgs.callPackage ./shell.nix {
          inherit craneLib;
          maelstrom = self.packages.${system}.default;
        };
      }
    );
}
