{
  description = "Meticulous";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
  };

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux = {
      default = nixpkgs.legacyPackages.x86_64-linux.callPackage ./package.nix { };
    };
  };
}
