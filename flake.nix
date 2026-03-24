{
  description = "Rust project";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    devshells.url = "github:vaporif/nix-devshells";
    devshells.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    nixpkgs,
    devshells,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-darwin"];
    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.alejandra);

    devShells = forAllSystems (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      baseShell = devshells.devShells.${system}.rust;
    in {
      default = pkgs.mkShell {
        inputsFrom = [baseShell];
        nativeBuildInputs = [pkgs.llvmPackages.clang pkgs.llvmPackages.libclang];
        LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
        BINDGEN_EXTRA_CLANG_ARGS = "-isysroot ${pkgs.apple-sdk_26.sdkroot}";
      };
    });
  };
}
