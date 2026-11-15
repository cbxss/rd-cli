//! Smoke tests against the real Real-Debrid API. They use your account (the
//! saved token, or `RD_LIVE_TOKEN`), so they only run when asked for:
//!
//!     RD_LIVE=1 cargo test --test live -- --ignored --nocapture --test-threads 1
//!
//! `live_read_only` only reads. `live_download_round_trip` adds a small, legal
//! torrent (Big Buck Bunny, ~280 MB; override with `RD_LIVE_MAGNET`),
//! downloads it twice (~560 MB of traffic) and deletes it afterwards unless it
//! was already on the account.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

const DEFAULT_MAGNET: &str = "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c\
    &dn=Big+Buck+Bunny&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337\
    &tr=udp%3A%2F%2Fexplodie.org%3A6969&tr=wss%3A%2F%2Ftracker.webtorrent.dev";

fn require_opt_in() {
    assert!(
        std::env::var("RD_LIVE").as_deref() == Ok("1"),
        "live tests use your Real-Debrid account; set RD_LIVE=1 to run them"
    );
}

struct Out {
    ok: bool,
    stdout: String,
    stderr: String,
}

fn rd(dir: &Path, args: &[&str]) -> Out {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rd"));
    cmd.args(args).current_dir(dir).stdin(Stdio::null());
    // Never inherit test-only overrides from the shell.
    for var in [
        "RD_API_URL",
        "RD_POLL_MS",
        "RD_CA_CERT",
        "RD_READ_TIMEOUT_SECS",
    ] {
        cmd.env_remove(var);
    }
    if let Ok(token) = std::env::var("RD_LIVE_TOKEN") {
        cmd.env("RD_TOKEN", token);
    }
    let started = Instant::now();
    let out = cmd.output().unwrap();
    let out = Out {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    };
    eprintln!(
        "--- rd {args:?} ({:.1}s)\n{}{}",
        started.elapsed().as_secs_f64(),
        out.stdout,
        out.stderr
    );
    out
}

fn torrents_with_hash(dir: &Path, hash: &str) -> Vec<serde_json::Value> {
    let out = rd(dir, &["torrents", "list", "--json"]);
    assert!(out.ok, "{}", out.stderr);
    let all: Vec<serde_json::Value> = serde_json::from_str(&out.stdout).unwrap();
    all.into_iter()
        .filter(|t| {
            t["hash"]
                .as_str()
                .is_some_and(|h| h.eq_ignore_ascii_case(hash))
        })
        .collect()
}

fn dir_size(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

fn hash_of(magnet: &str) -> String {
    magnet
        .split(['?', '&'])
        .find_map(|kv| kv.strip_prefix("xt=urn:btih:"))
        .expect("magnet without a btih hash")
        .to_ascii_lowercase()
}

/// Deletes the torrent on drop (also when an assertion fails).
struct Cleanup<'a> {
    dir: &'a Path,
    id: Option<String>,
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if let Some(id) = &self.id {
            rd(self.dir, &["torrents", "delete", id]);
        }
    }
}

#[test]
#[ignore]
fn live_read_only() {
    require_opt_in();
    let dir = tempfile::tempdir().unwrap();
    for args in [
        &["auth", "whoami", "--json"][..],
        &["torrents", "list", "--json"],
        &["downloads", "list", "--json"],
        &["traffic", "--json"],
        &["hosts", "--json"],
    ] {
        let out = rd(dir.path(), args);
        assert!(out.ok, "rd {args:?}: {}", out.stderr);
        serde_json::from_str::<serde_json::Value>(&out.stdout)
            .unwrap_or_else(|e| panic!("rd {args:?} printed invalid JSON: {e}"));
    }
    let out = rd(dir.path(), &["dl", "ZZZZZZZZZZZZZ"]);
    assert!(!out.ok);
    assert!(
        out.stderr.contains("not a torrent or download ID"),
        "{}",
        out.stderr
    );
}

#[test]
#[ignore]
fn live_download_round_trip() {
    require_opt_in();
    let magnet = std::env::var("RD_LIVE_MAGNET").unwrap_or_else(|_| DEFAULT_MAGNET.to_string());
    let hash = hash_of(&magnet);
    let dir = tempfile::tempdir().unwrap();
    let pre_existing = !torrents_with_hash(dir.path(), &hash).is_empty();
    let mut cleanup = Cleanup {
        dir: dir.path(),
        id: None,
    };

    // 1. Parallel download (-j 3) of every file.
    let out = rd(
        dir.path(),
        &[
            "dl",
            &magnet,
            "-o",
            "one",
            "-j",
            "3",
            "--stall-timeout",
            "20m",
        ],
    );
    let found = torrents_with_hash(dir.path(), &hash);
    if !pre_existing {
        cleanup.id = found
            .first()
            .and_then(|t| t["id"].as_str())
            .map(String::from);
    }
    assert!(out.ok, "{}", out.stderr);
    assert!(
        !out.stdout.contains("retrying"),
        "files failed in parallel: {}",
        out.stdout
    );
    let torrent = found
        .iter()
        .find(|t| t["status"] == "downloaded")
        .expect("torrent is downloaded");
    let id = torrent["id"].as_str().unwrap().to_string();
    let expected = torrent["bytes"].as_u64().unwrap();
    let got = dir_size(&dir.path().join("one"));
    assert_eq!(
        got, expected,
        "downloaded bytes vs. the torrent's selected size"
    );

    // 2. Rerun: same torrent, nothing fetched again.
    let out = rd(dir.path(), &["dl", &magnet, "-o", "one"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains("Reusing existing torrent"),
        "{}",
        out.stdout
    );
    assert!(out.stdout.contains("already downloaded"), "{}", out.stdout);
    assert_eq!(
        torrents_with_hash(dir.path(), &hash).len(),
        found.len(),
        "a duplicate torrent was added"
    );

    // 3. By ID, asking for far more connections than Real-Debrid allows per
    //    link: the per-link cap must keep this working.
    let out = rd(
        dir.path(),
        &["dl", &id, "-o", "two", "-j", "1", "--concurrency", "32"],
    );
    assert!(out.ok, "{}", out.stderr);
    assert_eq!(dir_size(&dir.path().join("two")), expected);
}
