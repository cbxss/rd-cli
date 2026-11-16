# rd

A [Real-Debrid](https://real-debrid.com) CLI.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/cbxss/rd-cli/main/install.sh | sh
```

This installs the latest GitHub Release to `~/.local/bin`.

For a specific version:

```bash
RD_INSTALL_VERSION=v0.2.0 sh -c "$(curl -fsSL https://raw.githubusercontent.com/cbxss/rd-cli/main/install.sh)"
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

Your token is stored in `config.toml` in your user config directory
(`~/.config/rd` on Linux, `~/Library/Application Support/rd` on macOS,
`%APPDATA%\rd\config` on Windows), readable only by you on Unix.

## Usage

Download almost anything with `rd dl`:

```bash
rd dl 'magnet:?xt=urn:btih:...'
rd dl ./movie.torrent
rd dl https://host.com/file.zip
rd dl <torrent-or-download-id>
rd dl -j 1 'magnet:?xt=urn:btih:...'   # one file at a time (default: 3)
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

## Environment

- `RD_TOKEN`: use this API token instead of the saved one
- `RD_CONFIG_DIR`: directory holding `config.toml`
- `RD_API_URL`: API base URL (the end-to-end tests point this at a fake server)
- `RD_POLL_MS`: torrent status poll interval in milliseconds (default 5000)
- `RD_CA_CERT`: extra PEM root certificate to trust (e.g. a TLS-inspecting proxy)
- `RD_READ_TIMEOUT_SECS`: give up on a connection that sends nothing for this long (default 60)

## Development

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

`tests/e2e.rs` runs the real binary against a fake Real-Debrid
(`tests/support/mod.rs`), so it needs no account or network access.

Two more suites only run when asked for:

```bash
# Docker: nginx, HTTPS with a private CA (Caddy), network faults
# (toxiproxy), and an undersized disk (tmpfs)
cargo test --features container-tests --test containers -- --test-threads 2

# Real Real-Debrid, using your account. live_download_round_trip adds a
# small legal torrent, downloads it twice (~560 MB) and deletes it again.
RD_LIVE=1 cargo test --test live -- --ignored --nocapture --test-threads 1
```
