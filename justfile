default:
    @just --list

check: clippy test check-fmt lint

lint: lint-toml check-typos check-nix-fmt lint-actions

fmt: fmt-rust fmt-toml fmt-nix

build *args:
    cargo build --workspace {{args}}

clippy:
    cargo clippy --workspace -- -D warnings

test:
    cargo nextest run --workspace

check-fmt:
    cargo fmt --all -- --check

fmt-rust:
    cargo fmt --all

lint-toml:
    taplo check

fmt-toml:
    taplo fmt

check-nix-fmt:
    alejandra --check flake.nix

fmt-nix:
    alejandra flake.nix

check-typos:
    typos

lint-actions:
    actionlint

run-node *args:
    cargo run -p mote-node -- node --chain etc/genesis.json {{args}}

run-analytics *args:
    cargo run -p mote-analytics -- {{args}}

run-all:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill 0' EXIT
    just run-node &
    sleep 2
    just run-analytics &
    wait
