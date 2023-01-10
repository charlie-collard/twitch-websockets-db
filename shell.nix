let
  unstable = import (fetchTarball {
    name = "unstable-2023-01-10";
    url = "https://github.com/nixos/nixpkgs/archive/35f1f865c03671a4f75a6996000f03ac3dc3e472.tar.gz";
    sha256 = "sha256:120cip1yn6g5hg35pwd07adr9h0i49g45pay77ix3yfr0nlcx2yh";
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
