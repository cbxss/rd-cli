# rd

Use [Real-Debrid](https://real-debrid.com) from your command line. A modern Rust
reimplementation of [`realdebrid-CLI`](https://github.com/MrMitch/realdebrid-CLI),
built against the current Real-Debrid REST API (`api.real-debrid.com/rest/1.0`)
instead of screen-scraping the website.

Built to move very large files (200GB+) efficiently: downloads stream straight
to disk and never buffer a whole file in memory, and large range-capable
downloads are split into concurrent segments for throughput. Interrupted
downloads resume rather than restarting from zero.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/cbxss/rd-cli/main/install.sh | sh
```

This installs the latest GitHub Release to `~/.local/bin`. To install a specific
tag or a fork:

```bash
RD_INSTALL_VERSION=v0.1.0 sh -c "$(curl -fsSL https://raw.githubusercontent.com/cbxss/rd-cli/main/install.sh)"
RD_INSTALL_REPO=owner/rd-cli sh -c "$(curl -fsSL https://raw.githubusercontent.com/owner/rd-cli/main/install.sh)"
```

You can also build from source:

```bash
cargo build --release
# binary at target/release/rd
```

Release builds are published from tags:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Getting started

Create a private API token at <https://real-debrid.com/apitoken>, then:

```bash
rd auth login      # paste your token when prompted
rd auth whoami      # confirm it worked
```

The token is stored at `~/.config/rd/config.toml` (mode `0600`).

## Usage

```bash
# One command, auto-detected: magnet / .torrent file / hoster link / file of links
rd dl 'magnet:?xt=urn:btih:...'
rd dl ./some.torrent
rd dl https://host.com/myFile.zip
rd dl -o ~/Downloads links.txt

# Unrestrict a hoster link and download it
rd unrestrict https://host.com/myFile.zip

# ... into a specific directory
rd unrestrict -o ~/Downloads https://host.com/myFile.zip

# Bulk mode: one link per line in a file
rd unrestrict links.txt

# Dry run: unrestrict without downloading
rd unrestrict -t links.txt

# Just print the resolved direct-download URLs
rd unrestrict -l links.txt > direct-links.txt

# Torrents
rd torrents add 'magnet:?xt=urn:btih:...'
rd torrents add ./some.torrent --host real-debrid.com
rd torrents list
rd torrents info <id>
rd torrents select <id> all        # selects files and auto-downloads once ready
rd torrents delete <id>

# Account
rd hosts                 # supported hosters
rd hosts --status        # hoster up/down status (requires auth)
rd traffic                # bandwidth usage
rd traffic --details --from 2026-07-01 --to 2026-07-31
rd downloads list
rd downloads delete <id>

# Shell completions
rd completions zsh > ~/.zfunc/_rd
```

Most listing commands accept `--json` for machine-readable output.

## Design notes

- **Download engine** (`src/download.rs`): probes range support, then either
  streams a single connection to disk or splits the file into N concurrent
  range requests writing to disjoint file offsets. A `<dest>.rdpart` sidecar
  tracks completed segments so an interrupted download resumes instead of
  restarting.
- **Auth**: a Real-Debrid private API token (Bearer auth), not the full OAuth2
  device flow — simplest option for personal use.
- Release builds are tuned for a small, dependency-light static binary
  (`opt-level = "z"`, LTO, stripped symbols).

## Development

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
```
