{
	description = "p2p telemetry dev environment";

	inputs = {
		nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

		rust-overlay = {
			url = "github:oxalica/rust-overlay";
			inputs.nixpkgs.follows = "nixpkgs";
		};
	};

	outputs = { self, nixpkgs, rust-overlay }:
		let
			system = "x86_64-linux";
			pkgs = import nixpkgs {
				inherit system;
				overlays = [ rust-overlay.overlays.default ];
			};
			rust-toolchain = pkgs.rust-bin.stable.latest.default;
		in
		{
			devShells.${system}.default = pkgs.mkShell {
				packages = [
					rust-toolchain
				];
			};
		};
}

