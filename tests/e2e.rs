//! End-to-end tests: the real `rd` binary against a fake Real-Debrid
//! (`tests/support`). Nothing here touches the network beyond localhost.

mod support;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use support::{Fake, TOKEN, TorrentSpec, data, step};

const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
const MIB: usize = 1024 * 1024;

fn magnet(hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{hash}&dn=test")
}

struct Env {
    fake: Fake,
    dir: tempfile::TempDir,
}

/// Kills the child process if a test fails while it's still running.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Out {
    ok: bool,
    stdout: String,
    stderr: String,
}

impl Env {
    fn new() -> Self {
        Env {
            fake: Fake::start(),
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn out_dir(&self) -> PathBuf {
        self.dir.path().join("out")
    }

    fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_rd"));
        c.args(args)
            .current_dir(self.dir.path())
            .env("RD_API_URL", self.fake.url())
            .env("RD_TOKEN", TOKEN)
            .env("RD_CONFIG_DIR", self.dir.path().join("config"))
            .env("RD_POLL_MS", "10")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }

    fn spawn(&self, args: &[&str]) -> Child {
        self.cmd(args).spawn().unwrap()
    }

    /// Runs `rd` to completion (killing it after 120 s so a hang fails the test).
    fn run(&self, args: &[&str]) -> Out {
        let mut child = self.spawn(args);
        let mut so = child.stdout.take().unwrap();
        let mut se = child.stderr.take().unwrap();
        let t1 = std::thread::spawn(move || {
            let mut s = String::new();
            so.read_to_string(&mut s).unwrap();
            s
        });
        let t2 = std::thread::spawn(move || {
            let mut s = String::new();
            se.read_to_string(&mut s).unwrap();
            s
        });
        let deadline = Instant::now() + Duration::from_secs(120);
        let status = loop {
            if let Some(st) = child.try_wait().unwrap() {
                break st;
            }
            if Instant::now() > deadline {
                child.kill().unwrap();
                panic!("rd {args:?} timed out");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let out = Out {
            ok: status.success(),
            stdout: t1.join().unwrap(),
            stderr: t2.join().unwrap(),
        };
        eprintln!("--- rd {args:?}\n{}{}", out.stdout, out.stderr);
        out
    }
}

fn assert_file(path: &Path, expected: &[u8]) {
    let got = std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert!(
        got.len() == expected.len() && got == expected,
        "{} differs",
        path.display()
    );
}

#[test]
fn cached_magnet_downloads_every_file() {
    let env = Env::new();
    let (a, b) = (data(3000, 1), data(5 * MIB, 2));
    env.fake.on_magnet(TorrentSpec::new(
        HASH,
        vec![("dir/a.txt", a.clone()), ("b.bin", b.clone())],
    ));

    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q"]);
    assert!(out.ok, "{}", out.stderr);
    assert_file(&env.out_dir().join("a.txt"), &a);
    assert_file(&env.out_dir().join("b.bin"), &b);
    assert_eq!(env.fake.add_count(), 1);
    assert_eq!(env.fake.premature_selects(), 0);
}

#[test]
fn slow_magnet_selects_after_conversion_and_waits() {
    let env = Env::new();
    let a = data(1000, 3);
    env.fake.on_magnet(
        TorrentSpec::new(HASH, vec![("a.bin", a.clone())])
            .initial(step("magnet_conversion", 0.0))
            .script(vec![
                step("magnet_conversion", 0.0),
                step("waiting_files_selection", 0.0),
                step("downloading", 10.0),
                step("downloading", 60.0),
                step("downloaded", 100.0),
            ]),
    );

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains("waiting on torrent: downloading (60%)"),
        "{}",
        out.stdout
    );
    assert_file(&env.out_dir().join("a.bin"), &a);
    assert_eq!(env.fake.premature_selects(), 0);
}

#[test]
fn rerun_reuses_torrent_and_skips_finished_files() {
    let env = Env::new();
    let a = data(2 * MIB, 4);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("a.bin", a.clone())]));

    assert!(env.run(&["dl", &magnet(HASH), "-o", "out"]).ok);
    env.fake.take_file_gets();

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains("Reusing existing torrent"),
        "{}",
        out.stdout
    );
    assert!(out.stdout.contains("already downloaded"), "{}", out.stdout);
    assert_eq!(
        env.fake.add_count(),
        1,
        "the magnet must not be added twice"
    );
    assert_eq!(
        env.fake.take_file_gets(),
        0,
        "no file data should be refetched"
    );
    assert_file(&env.out_dir().join("a.bin"), &a);
}

#[test]
fn copy_with_partial_selection_is_not_reused() {
    let env = Env::new();
    let (a, b) = (data(100, 5), data(200, 6));
    let files = vec![("a.bin", a.clone()), ("b.bin", b.clone())];
    env.fake.add_torrent(
        TorrentSpec::new(HASH, files.clone())
            .initial(step("downloaded", 100.0))
            .script(vec![])
            .selected(vec![true, false]),
    );
    env.fake.on_magnet(TorrentSpec::new(HASH, files));

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("Added torrent"), "{}", out.stdout);
    assert_file(&env.out_dir().join("b.bin"), &b);
}

#[test]
fn reuse_finds_torrents_beyond_the_first_page() {
    let env = Env::new();
    let a = data(100, 7);
    // Oldest torrent, so newest-first listing puts it on page 2.
    env.fake.add_torrent(
        TorrentSpec::new(HASH, vec![("a.bin", a.clone())])
            .initial(step("downloaded", 100.0))
            .script(vec![])
            .selected(vec![true]),
    );
    env.fake.add_filler_torrents(5200);

    let out = env.run(&["torrents", "list", "--json"]);
    assert!(out.ok, "{}", out.stderr);
    let list: Vec<serde_json::Value> = serde_json::from_str(&out.stdout).unwrap();
    assert_eq!(list.len(), 5201);

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert_eq!(env.fake.add_count(), 0);
    assert_file(&env.out_dir().join("a.bin"), &a);
}

#[test]
fn dl_accepts_torrent_and_download_ids() {
    let env = Env::new();
    let (a, b) = (data(100, 8), data(300, 9));
    let tid = env.fake.add_torrent(
        TorrentSpec::new(HASH, vec![("a.bin", a.clone())])
            .initial(step("downloaded", 100.0))
            .script(vec![])
            .selected(vec![true]),
    );
    let did = env.fake.add_download("b.bin", b.clone());

    assert!(env.run(&["dl", &tid, "-o", "out"]).ok);
    assert_file(&env.out_dir().join("a.bin"), &a);
    assert!(env.run(&["dl", &did, "-o", "out"]).ok);
    assert_file(&env.out_dir().join("b.bin"), &b);

    let out = env.run(&["dl", "ZZZZZZZZZZZZZ"]);
    assert!(!out.ok);
    assert!(
        out.stderr.contains("not a torrent or download ID"),
        "{}",
        out.stderr
    );
}

#[test]
fn failed_torrent_reports_delete_hint() {
    let env = Env::new();
    let tid = env.fake.add_torrent(
        TorrentSpec::new(HASH, vec![("a.bin", data(10, 10))])
            .initial(step("error", 0.0))
            .script(vec![]),
    );
    let out = env.run(&["dl", &tid]);
    assert!(!out.ok);
    assert!(
        out.stderr.contains(&format!("rd torrents delete {tid}")),
        "{}",
        out.stderr
    );
}

#[test]
fn stalled_torrent_gives_up() {
    let env = Env::new();
    let tid = env.fake.add_torrent(
        TorrentSpec::new(HASH, vec![("a.bin", data(10, 11))])
            .initial(step("downloading", 5.0))
            .script(vec![])
            .selected(vec![true]),
    );
    let started = Instant::now();
    let out = env.run(&["dl", &tid, "--stall-timeout", "1s"]);
    assert!(!out.ok);
    assert!(out.stderr.contains("made no progress"), "{}", out.stderr);
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[test]
fn dropped_connections_while_polling_are_retried() {
    let env = Env::new();
    let a = data(100, 12);
    env.fake.on_magnet(
        TorrentSpec::new(HASH, vec![("a.bin", a.clone())])
            .initial(step("magnet_conversion", 0.0))
            .script(vec![
                step("waiting_files_selection", 0.0),
                step("downloaded", 100.0),
            ]),
    );
    env.fake.faults(|f| f.drop_info_requests = 2);

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert_file(&env.out_dir().join("a.bin"), &a);
}

#[test]
fn torrents_add_select_waits_for_conversion() {
    let env = Env::new();
    env.fake.on_magnet(
        TorrentSpec::new(HASH, vec![("a.bin", data(100, 13))])
            .initial(step("magnet_conversion", 0.0))
            .script(vec![
                step("magnet_conversion", 0.0),
                step("waiting_files_selection", 0.0),
                step("downloading", 10.0),
            ]),
    );
    let out = env.run(&["torrents", "add", &magnet(HASH), "--select", "all"]);
    assert!(out.ok, "{}", out.stderr);
    assert_eq!(env.fake.premature_selects(), 0);
}

#[test]
fn server_ignoring_ranges_fails_cleanly() {
    let env = Env::new();
    env.fake.on_magnet(TorrentSpec::new(
        HASH,
        vec![("big.bin", data(60 * MIB, 14))],
    ));
    env.fake.faults(|f| f.ignore_ranges = true);

    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q"]);
    assert!(!out.ok);
    assert!(
        out.stderr.contains("ignored the range request"),
        "{}",
        out.stderr
    );
}

#[derive(serde::Deserialize)]
struct Sidecar {
    completed_ranges: Vec<(u64, u64)>,
}

#[test]
fn interrupted_download_resumes_where_it_stopped() {
    let env = Env::new();
    let big = data(140 * MIB, 15);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("big.bin", big.clone())]));
    env.fake.faults(|f| f.throttle_ms_per_mib = 20);

    let dest = env.out_dir().join("big.bin");
    let sidecar_path = env.out_dir().join("big.bin.rdpart");
    let mut child = KillOnDrop(env.spawn(&["dl", &magnet(HASH), "-o", "out", "-q"]));

    // Wait until at least one chunk is recorded as finished, then kill -9.
    let deadline = Instant::now() + Duration::from_secs(60);
    let done = loop {
        let sc = std::fs::read(&sidecar_path)
            .ok()
            .and_then(|d| serde_json::from_slice::<Sidecar>(&d).ok());
        if let Some(sc) = sc.filter(|s| !s.completed_ranges.is_empty()) {
            break sc.completed_ranges;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "rd exited before it was interrupted"
        );
        assert!(Instant::now() < deadline, "no chunk finished in time");
        std::thread::sleep(Duration::from_millis(10));
    };
    drop(child);
    let done_bytes: u64 = done.iter().map(|(s, e)| e - s + 1).sum();
    assert!(
        done_bytes < big.len() as u64,
        "everything finished before the kill"
    );

    env.fake.take_ranges();
    env.fake.faults(|f| f.throttle_ms_per_mib = 0);
    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q"]);
    assert!(out.ok, "{}", out.stderr);

    let refetched = env.fake.take_ranges();
    assert!(
        !refetched.is_empty(),
        "the rerun should fetch the missing chunks"
    );
    for (s, e) in &refetched {
        assert!(
            !done.iter().any(|(ds, de)| ds <= s && e <= de),
            "chunk {s}-{e} was finished before the kill but fetched again"
        );
    }
    assert_file(&dest, &big);
    assert!(!sidecar_path.exists());
}

#[test]
fn same_named_files_in_a_torrent_do_not_overwrite_each_other() {
    let env = Env::new();
    let (a, b) = (data(1000, 20), data(2000, 21));
    env.fake.on_magnet(TorrentSpec::new(
        HASH,
        vec![
            ("Season 1/sample.mkv", a.clone()),
            ("Season 2/sample.mkv", b.clone()),
        ],
    ));
    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-j", "3"]);
    assert!(out.ok, "{}", out.stderr);
    assert_file(&env.out_dir().join("sample.mkv"), &a);
    assert_file(&env.out_dir().join("sample (2).mkv"), &b);
}

#[test]
fn link_expiring_mid_download_stops_quickly() {
    let env = Env::new();
    env.fake.on_magnet(TorrentSpec::new(
        HASH,
        vec![("big.bin", data(400 * MIB, 22))],
    ));
    env.fake.faults(|f| f.fail_ranges_with = Some(403));

    let started = Instant::now();
    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q", "--concurrency", "2"]);
    assert!(!out.ok);
    assert!(out.stderr.contains("403"), "{}", out.stderr);
    // 7 chunks, 2 connections; a 403 isn't retried and the remaining chunks
    // are abandoned. The file gets one more pass (with a freshly unrestricted
    // link, which is what fixes a genuinely expired one): at most 2 + 2.
    let requests = env.fake.take_ranges().len();
    assert!(
        requests <= 4,
        "{requests} range requests after a permanent failure"
    );
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn interrupted_small_download_leaves_no_truncated_file() {
    let env = Env::new();
    let a = data(20 * MIB, 23);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("a.bin", a.clone())]));
    env.fake.faults(|f| f.throttle_ms_per_mib = 100);

    let dest = env.out_dir().join("a.bin");
    let tmp = env.out_dir().join("a.bin.rdtmp");
    let child = KillOnDrop(env.spawn(&["dl", &magnet(HASH), "-o", "out", "-q"]));
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::metadata(&tmp).map(|m| m.len()).unwrap_or(0) == 0 {
        assert!(Instant::now() < deadline, "download never started");
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(child);
    assert!(
        !dest.exists(),
        "a partial file must not appear under the real name"
    );

    env.fake.faults(|f| f.throttle_ms_per_mib = 0);
    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q"]);
    assert!(out.ok, "{}", out.stderr);
    assert_file(&dest, &a);
    assert!(!tmp.exists());
}

#[test]
fn unrestrict_json_keeps_stdout_machine_readable() {
    let env = Env::new();
    let a = data(500, 24);
    let tid = env.fake.add_torrent(
        TorrentSpec::new(HASH, vec![("a.bin", a.clone())])
            .initial(step("downloaded", 100.0))
            .script(vec![])
            .selected(vec![true]),
    );
    let link = format!("{}/hoster/{tid}/1", env.fake.url());
    let out = env.run(&["unrestrict", &link, "--json", "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    let v: serde_json::Value =
        serde_json::from_str(&out.stdout).expect("stdout is one JSON document");
    assert_eq!(v["filename"], "a.bin");
    assert_file(&env.out_dir().join("a.bin"), &a);
}

#[test]
fn add_magnet_5xx_after_creating_does_not_duplicate() {
    let env = Env::new();
    let a = data(1000, 30);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("a.bin", a.clone())]));
    env.fake.faults(|f| f.add_magnet_5xx_after_create = 1);

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert_eq!(env.fake.add_count(), 1, "addMagnet must not be re-sent");
    assert_eq!(env.fake.torrent_count(), 1, "no duplicate torrent");
    assert_file(&env.out_dir().join("a.bin"), &a);
}

#[test]
fn files_refused_in_parallel_are_retried_one_at_a_time() {
    let env = Env::new();
    let files: Vec<(String, Arc<Vec<u8>>)> = (0..5)
        .map(|i| (format!("f{i}.bin"), data(1000, 40 + i)))
        .collect();
    env.fake.on_magnet(TorrentSpec::new(
        HASH,
        files.iter().map(|(n, d)| (n.as_str(), d.clone())).collect(),
    ));
    // One file is refused before it gets a name, one after.
    env.fake.faults(|f| {
        f.refuse_unrestricts = 1;
        f.refuse_files = 1;
    });

    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-j", "3"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout
            .contains("retrying 2 failed file(s) one at a time"),
        "{}",
        out.stdout
    );
    for (name, d) in &files {
        assert_file(&env.out_dir().join(name), d);
    }
    // Retried files keep their names: no "f0 (2).bin".
    assert_eq!(
        std::fs::read_dir(env.out_dir()).unwrap().count(),
        files.len()
    );
}

#[test]
fn big_file_with_one_connection_still_uses_resumable_chunks() {
    let env = Env::new();
    let big = data(60 * MIB, 50);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("big.bin", big.clone())]));
    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q", "--concurrency", "1"]);
    assert!(out.ok, "{}", out.stderr);
    assert_file(&env.out_dir().join("big.bin"), &big);
    assert!(
        !env.fake.take_ranges().is_empty(),
        "downloaded without ranges (not resumable)"
    );
}

#[test]
fn an_unrelated_file_with_the_same_name_is_never_overwritten() {
    let env = Env::new();
    let a = data(3000, 51);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("a.bin", a.clone())]));
    std::fs::create_dir_all(env.out_dir()).unwrap();
    std::fs::write(env.out_dir().join("a.bin"), b"someone else's file").unwrap();

    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("saving as"), "{}", out.stdout);
    assert_eq!(
        std::fs::read(env.out_dir().join("a.bin")).unwrap(),
        b"someone else's file"
    );
    assert_file(&env.out_dir().join("a (2).bin"), &a);

    // A rerun recognises the numbered copy as already downloaded.
    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains("already downloaded: out/a (2).bin")
            || out.stdout.contains("already downloaded"),
        "{}",
        out.stdout
    );
    assert_eq!(std::fs::read_dir(env.out_dir()).unwrap().count(), 2);
}

#[test]
fn leftover_resume_file_is_cleared_by_a_successful_download() {
    let env = Env::new();
    let a = data(3000, 52);
    env.fake
        .on_magnet(TorrentSpec::new(HASH, vec![("a.bin", a.clone())]));
    std::fs::create_dir_all(env.out_dir()).unwrap();
    std::fs::write(env.out_dir().join("a.bin.rdpart"), b"{}").unwrap();

    assert!(env.run(&["dl", &magnet(HASH), "-o", "out", "-q"]).ok);
    assert_file(&env.out_dir().join("a.bin"), &a);
    assert!(!env.out_dir().join("a.bin.rdpart").exists());
    env.fake.take_file_gets();
    let out = env.run(&["dl", &magnet(HASH), "-o", "out"]);
    assert!(out.stdout.contains("already downloaded"), "{}", out.stdout);
    assert_eq!(env.fake.take_file_gets(), 0);
}

#[test]
fn bulk_unrestrict_names_json_and_output_file() {
    let env = Env::new();
    // Same size, different content: only the in-batch name check keeps the
    // second from being taken for an already-downloaded copy of the first.
    let (a, b) = (data(100, 53), data(100, 54));
    let t1 = env.fake.add_torrent(
        TorrentSpec::new(HASH, vec![("x/same.bin", a.clone())])
            .initial(step("downloaded", 100.0))
            .script(vec![])
            .selected(vec![true]),
    );
    let t2 = env.fake.add_torrent(
        TorrentSpec::new(
            "ffffffffffffffffffffffffffffffffffffffff",
            vec![("y/same.bin", b.clone())],
        )
        .initial(step("downloaded", 100.0))
        .script(vec![])
        .selected(vec![true]),
    );
    let base = env.fake.url();
    let list = env.dir.path().join("links.txt");
    std::fs::write(
        &list,
        format!("{base}/hoster/{t1}/1\n{base}/hoster/{t2}/1\n"),
    )
    .unwrap();

    let out = env.run(&["unrestrict", "links.txt", "-o", "out", "-q"]);
    assert!(out.ok, "{}", out.stderr);
    assert_file(&env.out_dir().join("same.bin"), &a);
    assert_file(&env.out_dir().join("same (2).bin"), &b);

    // A links file prints an array, even with a single link in it.
    std::fs::write(&list, format!("{base}/hoster/{t1}/1\n")).unwrap();
    let out = env.run(&["unrestrict", "links.txt", "--json", "-t"]);
    assert!(out.ok, "{}", out.stderr);
    let v: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    assert!(v.is_array(), "{v}");

    let out = env.run(&["unrestrict", "links.txt", "-O", "named.bin"]);
    assert!(!out.ok);
    assert!(
        out.stderr.contains("only works with a single link"),
        "{}",
        out.stderr
    );
}

#[test]
fn torrents_select_on_a_finished_torrent_downloads_nothing() {
    let env = Env::new();
    let tid = env.fake.add_torrent(
        TorrentSpec::new(
            HASH,
            vec![("a.bin", data(100, 55)), ("b.bin", data(100, 56))],
        )
        .initial(step("downloaded", 100.0))
        .script(vec![])
        .selected(vec![true, true]),
    );
    let out = env.run(&["torrents", "select", &tid, "1"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains(&format!("rd dl {tid}")),
        "{}",
        out.stdout
    );
    assert_eq!(env.fake.take_file_gets(), 0);
}

#[cfg(unix)]
#[test]
fn closed_stdout_exits_quietly() {
    let env = Env::new();
    env.fake.add_filler_torrents(3000); // far more output than a pipe buffer
    let mut child = KillOnDrop(env.spawn(&["torrents", "list", "--json"]));
    drop(child.0.stdout.take()); // like `| head` exiting
    let status = child.0.wait().unwrap();
    let mut stderr = String::new();
    child
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(!stderr.contains("panicked"), "{stderr}");
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(13),
        "expected SIGPIPE, got {status:?}"
    );
}

/// Download throughput against the local fake. Not a pass/fail test:
/// `cargo test --release --test e2e throughput -- --ignored --nocapture`
#[test]
#[ignore]
fn throughput() {
    let env = Env::new();
    let size: usize = std::env::var("RD_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    env.fake.on_magnet(TorrentSpec::new(
        HASH,
        vec![("big.bin", data(size * MIB, 99))],
    ));
    let started = Instant::now();
    let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q"]);
    let secs = started.elapsed().as_secs_f64();
    assert!(out.ok, "{}", out.stderr);
    println!(
        "THROUGHPUT {size} MiB in {secs:.2}s = {:.0} MiB/s",
        size as f64 / secs
    );
}

/// Many small files over a simulated 50 ms round trip, one at a time vs. in
/// parallel: `cargo test --release --test e2e many_files -- --ignored --nocapture`
#[test]
#[ignore]
fn many_files_bench() {
    for jobs in ["1", "3", "6"] {
        let env = Env::new();
        let files: Vec<(String, Arc<Vec<u8>>)> = (0..40)
            .map(|i| (format!("f{i}.bin"), data(MIB, i)))
            .collect();
        env.fake.on_magnet(TorrentSpec::new(
            HASH,
            files.iter().map(|(n, d)| (n.as_str(), d.clone())).collect(),
        ));
        env.fake.faults(|f| f.latency_ms = 50);
        let started = Instant::now();
        let out = env.run(&["dl", &magnet(HASH), "-o", "out", "-q", "-j", jobs]);
        assert!(out.ok, "{}", out.stderr);
        println!(
            "MANYFILES jobs={jobs}: 40 files in {:.2}s",
            started.elapsed().as_secs_f64()
        );
    }
}
