//! Running the `rd` binary from tests, and checking what it left on disk.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
pub const MIB: usize = 1024 * 1024;

pub fn magnet(hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{hash}&dn=test")
}

/// Settings from the developer's shell that would change how `rd` behaves
/// under test.
pub const AMBIENT_ENV: &[&str] = &[
    "RD_API_URL",
    "RD_TOKEN",
    "RD_CONFIG_DIR",
    "RD_POLL_MS",
    "RD_CA_CERT",
    "RD_READ_TIMEOUT_SECS",
];

pub struct Out {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Runs `cmd` to completion, killing it (and failing the test) after
/// `timeout`, so a hang can't stall the suite.
pub fn run(mut cmd: Command, timeout: Duration) -> Out {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let label = format!("{:?}", cmd.get_args().collect::<Vec<_>>());
    let mut child = cmd.spawn().unwrap();
    let mut so = child.stdout.take().unwrap();
    let mut se = child.stderr.take().unwrap();
    let read_out = std::thread::spawn(move || {
        let mut s = String::new();
        so.read_to_string(&mut s).unwrap();
        s
    });
    let read_err = std::thread::spawn(move || {
        let mut s = String::new();
        se.read_to_string(&mut s).unwrap();
        s
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("rd {label} did not finish within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let out = Out {
        ok: status.success(),
        stdout: read_out.join().unwrap(),
        stderr: read_err.join().unwrap(),
    };
    eprintln!("--- rd {label}\n{}{}", out.stdout, out.stderr);
    out
}

pub fn assert_file(path: &Path, expected: &[u8]) {
    let got = std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert!(
        got.len() == expected.len() && got == expected,
        "{} differs",
        path.display()
    );
}

#[derive(serde::Deserialize)]
struct Sidecar {
    completed_ranges: Vec<(u64, u64)>,
}

/// Byte ranges a `.rdpart` sidecar records as downloaded (none if absent or
/// unreadable).
pub fn completed_ranges(sidecar: &Path) -> Vec<(u64, u64)> {
    std::fs::read(sidecar)
        .ok()
        .and_then(|d| serde_json::from_slice::<Sidecar>(&d).ok())
        .map_or(Vec::new(), |s| s.completed_ranges)
}

pub fn completed_bytes(sidecar: &Path) -> u64 {
    completed_ranges(sidecar)
        .iter()
        .map(|(a, b)| b - a + 1)
        .sum()
}
