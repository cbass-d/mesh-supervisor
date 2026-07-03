#!/usr/bin/env bash
set -euo pipefail

# One-liner installer for mesh-supervisor release binaries.
# Downloads the latest GitHub release for the current OS/arch and installs it.

REPO="cbass-d/mesh-supervisor"
BIN="mesh-supervisor"

main() {
    local os arch target tmp_dir install_dir use_sudo=false

    os=$(uname -s)
    arch=$(uname -m)

    case "$os" in
        Linux)
            case "$arch" in
                x86_64) target="x86_64-unknown-linux-gnu" ;;
                *) err "unsupported architecture: $arch (Linux builds are x86_64 only)" ;;
            esac
            ;;
        Darwin)
            case "$arch" in
                x86_64) target="x86_64-apple-darwin" ;;
                arm64)  target="aarch64-apple-darwin" ;;
                *) err "unsupported architecture: $arch" ;;
            esac
            ;;
        *) err "unsupported OS: $os" ;;
    esac

    local asset="${BIN}-${target}"
    local version
    version=$(fetch_latest_version)
    say "Installing ${BIN} ${version} for ${target}"

    tmp_dir=$(mktemp -d)
    # ${tmp_dir:-}: the EXIT trap runs at top level, where main's locals are
    # out of scope — a bare $tmp_dir would abort under `set -u`.
    trap 'rm -rf "${tmp_dir:-}"' EXIT

    download "https://github.com/${REPO}/releases/download/${version}/${asset}" "${tmp_dir}/${asset}"
    download "https://github.com/${REPO}/releases/download/${version}/${asset}.sha256" "${tmp_dir}/${asset}.sha256"
    verify_checksum "$tmp_dir" "$asset"
    chmod +x "${tmp_dir}/${asset}"

    install_dir="/usr/local/bin"
    if ! [[ -w "$install_dir" ]]; then
        if command -v sudo >/dev/null 2>&1; then
            use_sudo=true
        else
            install_dir="${HOME}/.local/bin"
            mkdir -p "$install_dir"
            say "No write access to /usr/local/bin; installing to ${install_dir}"
            say "Add ${install_dir} to your PATH if it is not already"
        fi
    fi

    local dest="${install_dir}/${BIN}"
    if "$use_sudo"; then
        sudo install -m 755 "${tmp_dir}/${asset}" "$dest"
    else
        install -m 755 "${tmp_dir}/${asset}" "$dest"
    fi

    say "Installed ${BIN} to ${dest}"
    say "Run '${BIN} --help' to get started"
}

fetch_latest_version() {
    local url="https://api.github.com/repos/${REPO}/releases/latest"
    local json
    if command -v curl >/dev/null 2>&1; then
        json=$(curl -fsSL "$url")
    elif command -v wget >/dev/null 2>&1; then
        json=$(wget -qO- "$url")
    else
        err "curl or wget is required"
    fi

    local tag
    tag=$(printf '%s' "$json" | grep -o '"tag_name": *"[^"]*"' | head -n 1 | sed 's/.*"\([^"]*\)".*/\1/')
    if [[ -z "$tag" ]]; then
        err "could not determine latest release version"
    fi
    printf '%s' "$tag"
}

download() {
    local url=$1 out=$2
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$out"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$out"
    else
        err "curl or wget is required"
    fi
}

# Check the downloaded binary against the .sha256 published with the release.
# The checksum file holds a bare filename, so -c must run inside the directory.
verify_checksum() {
    local dir=$1 asset=$2
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$dir" && sha256sum -c "${asset}.sha256" >/dev/null 2>&1) \
            || err "checksum verification failed for ${asset}"
    elif command -v shasum >/dev/null 2>&1; then
        (cd "$dir" && shasum -a 256 -c "${asset}.sha256" >/dev/null 2>&1) \
            || err "checksum verification failed for ${asset}"
    else
        err "sha256sum or shasum is required to verify the download"
    fi
    say "Checksum verified"
}

say() {
    printf 'install.sh: %s\n' "$1"
}

err() {
    printf 'install.sh: error: %s\n' "$1" >&2
    exit 1
}

main "$@"
