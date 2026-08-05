#!/bin/sh
set -eu

repo="${RD_INSTALL_REPO:-cbxss/rd-cli}"
version="${RD_INSTALL_VERSION:-latest}"
bin_dir="${RD_INSTALL_DIR:-$HOME/.local/bin}"
binary="rd"

need() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "error: required command not found: $1" >&2
        exit 1
    }
}

need curl
need tar

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
    Linux) platform="unknown-linux-gnu" ;;
    Darwin) platform="apple-darwin" ;;
    *)
        echo "error: unsupported OS: $os" >&2
        exit 1
        ;;
esac

case "$arch" in
    x86_64|amd64) cpu="x86_64" ;;
    arm64|aarch64) cpu="aarch64" ;;
    *)
        echo "error: unsupported architecture: $arch" >&2
        exit 1
        ;;
esac

target="${cpu}-${platform}"
archive="rd-${target}.tar.gz"
checksums="SHA256SUMS-${target}.txt"

if [ "$version" = "latest" ]; then
    url="https://github.com/${repo}/releases/latest/download/${archive}"
    sums_url="https://github.com/${repo}/releases/latest/download/${checksums}"
else
    url="https://github.com/${repo}/releases/download/${version}/${archive}"
    sums_url="https://github.com/${repo}/releases/download/${version}/${checksums}"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

echo "Downloading $url"
curl -fsSL "$url" -o "$tmp_dir/$archive"
curl -fsSL "$sums_url" -o "$tmp_dir/$checksums"

(
    cd "$tmp_dir"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -c "$checksums"
    else
        shasum -a 256 -c "$checksums"
    fi
)

mkdir -p "$bin_dir"
tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"
install -m 0755 "$tmp_dir/$binary" "$bin_dir/$binary"

echo "Installed rd to $bin_dir/$binary"
case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *) echo "Add $bin_dir to PATH to run rd from any directory." ;;
esac
