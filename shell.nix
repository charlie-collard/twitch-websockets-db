let
  unstable = import (fetchTarball {
    name = "unstable-2023-01-10";
    url = "https://github.com/nixos/nixpkgs/archive/3c7487575d9445185249a159046cc02ff364bff8.tar.gz";
    sha256 = "sha256:0sll858mrfx64g5hc3sysg5cz4py9nxi8g7m9j5idhh8yq8lcz5p";
  }) { };
in

{ pkgs ? import <nixpkgs> {} }:

unstable.mkShell {
  nativeBuildInputs = with unstable; [ 
    (python3.withPackages (pyPkgs: with pyPkgs; [
      requests
      websockets
    ]))
  ];
}
