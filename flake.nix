{
  description = "Twitch websocket listener";

  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs/nixos-unstable";
    };
  };

  outputs = { self, nixpkgs, ... }: {
    devShells = nixpkgs.lib.genAttrs ["aarch64-darwin"] (system: {
      default = nixpkgs.legacyPackages.${system}.mkShell {
        nativeBuildInputs = with nixpkgs.legacyPackages.${system}; [
          (python3.withPackages (pyPkgs: with pyPkgs; [
            requests
            websockets
          ]))
        ];
      };
    });
  };
}
