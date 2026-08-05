# rd

A [Real-Debrid](https://real-debrid.com) CLI.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/cbxss/rd-cli/main/install.sh | sh
```

This installs the latest GitHub Release to `~/.local/bin`.

For a specific version:

```bash
RD_INSTALL_VERSION=v0.1.1 sh -c "$(curl -fsSL https://raw.githubusercontent.com/cbxss/rd-cli/main/install.sh)"
```

Build from source:

```bash
cargo build --release
```

## Login

Create a private API token at <https://real-debrid.com/apitoken>, then:

```bash
rd auth login
rd auth whoami
```

Your token is stored at `~/.config/rd/config.toml` with `0600` permissions on
Unix systems.

## Usage

Download almost anything with `rd dl`:

```bash
rd dl 'magnet:?xt=urn:btih:...'
rd dl ./movie.torrent
rd dl https://host.com/file.zip
rd dl -o ~/Downloads links.txt
```

Unrestrict hoster links:

```bash
rd unrestrict https://host.com/file.zip
rd unrestrict -o ~/Downloads links.txt
rd unrestrict -l links.txt > direct-links.txt
```

Manage torrents:

```bash
rd torrents add 'magnet:?xt=urn:btih:...'
rd torrents list
rd torrents info <id>
rd torrents select <id> all
rd torrents delete <id>
```

Check account and service info:

```bash
rd hosts
rd hosts --status
rd traffic
rd downloads list
```

Most listing commands accept `--json`.

## Shell Completions

```bash
rd completions zsh > ~/.zfunc/_rd
```

## Development

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```
