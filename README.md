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
rd dl --select 2,5 'magnet:?xt=urn:btih:...'   # only these files (ids from `rd torrents info`)
rd dl -O movie.mkv https://host.com/f.mkv      # name the file (single-file downloads only)
rd dl -l 'magnet:?xt=urn:btih:...'             # print direct links, download nothing
```

### Resuming

Downloads of 50 MiB or more resume. While one is in progress (or after it
was interrupted) the file is already at its full size, with a
`<name>.rdpart` file next to it recording which parts are done. A full-size
file is not a finished one while its `.rdpart` exists. To continue, rerun the
same command: finished files are skipped and partial ones pick up where they
stopped. Don't delete either file, or the progress is lost. Smaller files
restart from scratch.

When progress bars can't be drawn (stderr isn't a terminal: redirected to
a log, `nohup`), a plain progress line is printed every 30 seconds instead of a progress bar, plus a
`finished: <path>` line for each file.

### Scripting

- `rd dl --json` prints one JSON document when done:
  `{"files": [{"path", "bytes"}], "failed": [{"link", "error", "retryable"}]}`.
- `rd dl -l` and `rd unrestrict -l` print direct links, one per line.
- `rd torrents info --json`, `rd torrents add --json` (`{"id": ...}`) and most
  listing commands accept `--json`.
- Status messages go to stdout and errors to stderr; `-q`, `--json` and `-l`
  keep stdout clean.

Exit codes:

| Code | Meaning |
| ---- | ------- |
| 0 | Success |
| 1 | Error (bad id, authentication, dead link, disk full, ...) |
| 2 | Invalid usage (unknown flag, bad `--select`, ...) |
| 3 | Incomplete: a download failed in a way a rerun may fix (dropped connection, expired link, Real-Debrid busy) or a torrent stalled. Rerun the same command to continue. |

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
rd torrents select <id> all       # or file ids: 2,5
rd torrents list --filter active
rd torrents delete <id>
```

Check account and service info:

```bash
rd hosts
rd hosts --status
rd traffic
rd traffic --details --from 2026-11-01 --to 2026-11-15
rd downloads list
```


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
- `RD_PROGRESS_MS`: interval between plain progress lines when output isn't a terminal (default 30000)

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
