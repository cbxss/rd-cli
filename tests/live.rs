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
    rd_with(dir, args, true)
}

/// `echo: false` keeps account listings out of the test output.
fn rd_with(dir: &Path, args: &[&str], echo: bool) -> Out {
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
    let shown = if echo {
        out.stdout.as_str()
    } else {
        "(output hidden)\n"
    };
    eprintln!(
        "--- rd {args:?} ({:.1}s)\n{shown}{}",
        started.elapsed().as_secs_f64(),
        out.stderr
    );
    out
}

fn torrents_with_hash(dir: &Path, hash: &str) -> Vec<serde_json::Value> {
    let out = rd_with(dir, &["torrents", "list", "--json"], false);
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

/// On drop (also when an assertion fails), deletes every torrent with the
/// test's hash that wasn't on the account before the test started.
struct Cleanup<'a> {
    dir: &'a Path,
    hash: String,
    before: Vec<String>,
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let Ok(list) = std::panic::catch_unwind(|| torrents_with_hash(self.dir, &self.hash)) else {
            eprintln!(
                "cleanup: couldn't list torrents; check `rd torrents list` for {}",
                self.hash
            );
            return;
        };
        for t in list {
            if let Some(id) = t["id"]
                .as_str()
                .filter(|id| !self.before.iter().any(|b| b == id))
            {
                rd(self.dir, &["torrents", "delete", id]);
            }
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
        let out = rd_with(dir.path(), args, false);
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
    let before: Vec<String> = torrents_with_hash(dir.path(), &hash)
        .iter()
        .filter_map(|t| t["id"].as_str().map(String::from))
        .collect();
    let _cleanup = Cleanup {
        dir: dir.path(),
        hash: hash.clone(),
        before,
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
    let one = dir.path().join("one");
    let names: Vec<String> = std::fs::read_dir(&one)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let got = dir_size(&one);
    // Real-Debrid sometimes delivers a multi-file torrent as one .rar
    // (stored, uncompressed): the content plus a little archive overhead.
    let archived = names.len() == 1 && names[0].ends_with(".rar");
    eprintln!(
        "delivered as {}: {names:?}, {got} bytes for {expected} selected",
        if archived {
            "an archive"
        } else {
            "separate files"
        }
    );
    let ok = if archived {
        got >= expected && got - expected < 1024 * 1024
    } else {
        got == expected
    };
    if !ok {
        rd(dir.path(), &["torrents", "info", &id]);
        panic!("downloaded {got} bytes in {names:?}, torrent has {expected} selected");
    }

    // 2. Rerun: same torrent, nothing fetched again.
    let out = rd(dir.path(), &["dl", &magnet, "-o", "one"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains("reusing existing torrent"),
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
    assert_eq!(
        dir_size(&dir.path().join("two")),
        got,
        "second download differs"
    );
}
