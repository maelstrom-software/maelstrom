{ lib
, stdenv
, binaryen
, openssl
, pkg-config
, rustPlatform
, rustc-wasm32
, wasm-bindgen-cli
}:

let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
in

rustPlatform.buildRustPackage {
  pname = "meticulous";
  version = cargoToml.workspace.package.version;

  src = ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  prePatch = ''
    patchShebangs crates/maelstrom-web/build.sh
  '';

  # NOTE: we need to force lld otherwise rust-lld is not found for wasm32 target
  env.CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER = "lld";

  nativeBuildInputs = [
    binaryen
    pkg-config
    rustc-wasm32
    rustc-wasm32.llvmPackages.lld
    wasm-bindgen-cli
  ];

  buildInputs = [ openssl ];

  postInstall = ''
    rm $out/lib/libmaelstrom_web.so
    rmdir $out/lib
  '';

  doCheck = false;

  meta = with lib; {
    description = "Meticulous clustered job runner";
    homepage = "https://github.com/meticulous-software/meticulous";
    license = licenses.bsd3;
    maintainers = with maintainers; [ philiptaron ];
  };
}
