//! Docker-backed tests for the download engine: a real web server (nginx),
//! real TLS (Caddy with a private CA), real network faults (toxiproxy), and a
//! real undersized disk (a tmpfs in a container). Real-Debrid's API is still
//! the in-process fake; only the file downloads go through containers.
//!
//! Needs Docker; not part of the normal test run:
//!     cargo test --features container-tests --test containers -- --test-threads 2
//! (First run pulls nginx, caddy, toxiproxy and debian images.)

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use support::{Fake, TOKEN, TorrentSpec, data};
use testcontainers::core::{ExecCommand, IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, GenericImage, ImageExt};

const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
const MIB: usize = 1024 * 1024;

fn magnet() -> String {
    format!("magnet:?xt=urn:btih:{HASH}&dn=test")
}

fn unique(prefix: &str) -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    format!(
        "rd-{prefix}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

// ---- containers ----

fn with_files(
    mut req: ContainerRequest<GenericImage>,
    dir: &str,
    files: &[(&str, Arc<Vec<u8>>)],
) -> ContainerRequest<GenericImage> {
    for (name, bytes) in files {
        req = req.with_copy_to(format!("{dir}/{name}"), bytes.to_vec());
    }
    req
}

struct Web {
    container: Container<GenericImage>,
    name: String,
}

impl Web {
    fn url(&self) -> String {
        let port = self.container.get_host_port_ipv4(80.tcp()).unwrap();
        format!("http://127.0.0.1:{port}")
    }
}

/// nginx serving `files` from its web root.
fn nginx(files: &[(&str, Arc<Vec<u8>>)], network: Option<&str>) -> Web {
    let name = unique("nginx");
    let mut req = GenericImage::new("nginx", "1.27-alpine")
        .with_exposed_port(80.tcp())
        .with_wait_for(WaitFor::message_on_stdout("ready for start up"))
        .with_container_name(&name);
    if let Some(net) = network {
        req = req.with_network(net);
    }
    let container = with_files(req, "/usr/share/nginx/html", files)
        .start()
        .expect("failed to start nginx — is Docker running?");
    let web = Web { container, name };
    wait_http(&web.url());
    web
}

/// Caddy serving `files` over HTTPS with its own private CA. Returns the
/// container and the CA's root certificate (PEM).
fn caddy_tls(files: &[(&str, Arc<Vec<u8>>)]) -> (Container<GenericImage>, Vec<u8>) {
    let caddyfile = "{\n  local_certs\n  skip_install_trust\n  admin off\n}\n\
                     https://localhost:8443 {\n  tls internal\n  root * /srv\n  file_server\n}\n";
    let req = GenericImage::new("caddy", "2-alpine")
        .with_exposed_port(8443.tcp())
        // The CA file appears before the site certificate is issued; wait
        // for the certificate so the first handshake doesn't race it.
        .with_wait_for(WaitFor::message_on_stderr(
            "certificate obtained successfully",
        ))
        .with_container_name(unique("caddy"))
        .with_copy_to("/etc/caddy/Caddyfile", caddyfile.as_bytes().to_vec());
    let container = with_files(req, "/srv", files).start().expect("start caddy");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut out = container
            .exec(ExecCommand::new([
                "cat",
                "/data/caddy/pki/authorities/local/root.crt",
            ]))
            .unwrap();
        let pem = out.stdout_to_vec().unwrap();
        if pem.starts_with(b"-----BEGIN CERTIFICATE") {
            return (container, pem);
        }
        assert!(Instant::now() < deadline, "caddy never created its CA");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// toxiproxy forwarding its port 8666 to `upstream` on `network`.
struct Toxi {
    container: Container<GenericImage>,
}

impl Toxi {
    fn start(network: &str, upstream: &str) -> Self {
        let container = GenericImage::new("ghcr.io/shopify/toxiproxy", "2.12.0")
            .with_exposed_port(8474.tcp())
            .with_exposed_port(8666.tcp())
            .with_network(network)
            .with_container_name(unique("toxiproxy"))
            .start()
            .expect("start toxiproxy");
        let toxi = Toxi { container };
        wait_http(&format!("http://127.0.0.1:{}/version", toxi.api_port()));
        toxi.api(
            "POST",
            "/proxies",
            &format!(r#"{{"name":"web","listen":"0.0.0.0:8666","upstream":"{upstream}"}}"#),
        );
        toxi
    }

    fn api_port(&self) -> u16 {
        self.container.get_host_port_ipv4(8474.tcp()).unwrap()
    }

    fn url(&self) -> String {
        format!(
            "http://127.0.0.1:{}",
            self.container.get_host_port_ipv4(8666.tcp()).unwrap()
        )
    }

    fn add_toxic(&self, json: &str) {
        self.api("POST", "/proxies/web/toxics", json);
    }

    fn remove_toxic(&self, name: &str) {
        self.api("DELETE", &format!("/proxies/web/toxics/{name}"), "");
    }

    fn api(&self, method: &str, path: &str, body: &str) {
        let resp = raw_http(self.api_port(), method, path, body);
        assert!(
            resp.starts_with("HTTP/1.1 2"),
            "toxiproxy {method} {path} failed: {resp}"
        );
    }
}

/// A Debian container on the host network (so it reaches the fake API and
/// the web containers at 127.0.0.1) with `rd` installed and a tmpfs of
/// `disk` mounted at /out.
fn rd_box(disk: &str) -> Container<GenericImage> {
    let container = GenericImage::new("debian", "trixie-slim")
        .with_cmd(["sleep", "infinity"])
        .with_network("host")
        .with_mount(Mount::tmpfs_mount("/out").with_size(disk))
        .with_copy_to("/usr/local/bin/rd", PathBuf::from(env!("CARGO_BIN_EXE_rd")))
        .with_container_name(unique("rdbox"))
        .start()
        .expect("start debian");
    exec(&container, &["chmod", "+x", "/usr/local/bin/rd"]);
    container
}

struct ExecOut {
    code: i64,
    stdout: String,
    stderr: String,
}

fn exec(c: &Container<GenericImage>, argv: &[&str]) -> ExecOut {
    let mut res = c.exec(ExecCommand::new(argv.iter().copied())).unwrap();
    let stdout = String::from_utf8_lossy(&res.stdout_to_vec().unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&res.stderr_to_vec().unwrap()).into_owned();
    let code = loop {
        if let Some(code) = res.exit_code().unwrap() {
            break code;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    eprintln!("--- {argv:?} -> {code}\n{stdout}{stderr}");
    ExecOut {
        code,
        stdout,
        stderr,
    }
}

fn exec_bytes(c: &Container<GenericImage>, argv: &[&str]) -> Vec<u8> {
    c.exec(ExecCommand::new(argv.iter().copied()))
        .unwrap()
        .stdout_to_vec()
        .unwrap()
}

fn rd_in_box(c: &Container<GenericImage>, fake: &Fake, args: &[&str]) -> ExecOut {
    let api = format!("RD_API_URL={}", fake.url());
    let token = format!("RD_TOKEN={TOKEN}");
    let mut argv = vec![
        "env",
        &api,
        &token,
        "RD_CONFIG_DIR=/tmp/rd",
        "RD_POLL_MS=10",
        "rd",
    ];
    argv.extend_from_slice(args);
    exec(c, &argv)
}

// ---- host-side helpers ----

fn raw_http(port: u16, method: &str, path: &str, body: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    out
}

fn wait_http(url: &str) {
    let rest = url.strip_prefix("http://127.0.0.1:").unwrap();
    let (port, path) = rest
        .split_once('/')
        .map_or((rest, "/".to_string()), |(p, r)| (p, format!("/{r}")));
    let port: u16 = port.parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ok = std::panic::catch_unwind(|| raw_http(port, "GET", &path, ""))
            .is_ok_and(|r| r.starts_with("HTTP/1.1 2") || r.starts_with("HTTP/1.1 4"));
        if ok {
            return;
        }
        assert!(Instant::now() < deadline, "{url} never came up");
        std::thread::sleep(Duration::from_millis(200));
    }
}

struct Out {
    ok: bool,
    stdout: String,
    stderr: String,
}

/// Runs the host `rd` binary against `fake`, with extra environment.
fn rd(fake: &Fake, dir: &Path, env: &[(&str, &str)], args: &[&str]) -> Out {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rd"));
    cmd.args(args)
        .current_dir(dir)
        .env("RD_API_URL", fake.url())
        .env("RD_TOKEN", TOKEN)
        .env("RD_CONFIG_DIR", dir.join("config"))
        .env("RD_POLL_MS", "10")
        .env_remove("RD_CA_CERT")
        .stdin(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    let out = Out {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    };
    eprintln!("--- rd {args:?}\n{}{}", out.stdout, out.stderr);
    out
}

fn assert_file(path: &Path, expected: &[u8]) {
    let got = std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert!(
        got.len() == expected.len() && got == expected,
        "{} differs",
        path.display()
    );
}

fn torrent(files: &[(&str, Arc<Vec<u8>>)]) -> TorrentSpec {
    TorrentSpec::new(HASH, files.to_vec())
}

// ---- tests ----

#[test]
fn nginx_segmented_download_then_skip() {
    let big = data(300 * MIB, 1);
    let files = [("big.bin", big.clone())];
    let web = nginx(&files, None);
    let fake = Fake::start();
    fake.on_magnet(torrent(&files));
    fake.serve_files_from(&web.url());
    let dir = tempfile::tempdir().unwrap();

    let out = rd(
        &fake,
        dir.path(),
        &[],
        &["dl", &magnet(), "-o", "out", "-q"],
    );
    assert!(out.ok, "{}", out.stderr);
    assert_file(&dir.path().join("out/big.bin"), &big);

    let out = rd(&fake, dir.path(), &[], &["dl", &magnet(), "-o", "out"]);
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("already downloaded"), "{}", out.stdout);
}

#[test]
fn https_with_a_private_ca() {
    let file = data(80 * MIB, 2);
    let files = [("tls.bin", file.clone())];
    let (caddy, root_pem) = caddy_tls(&files);
    let port = caddy.get_host_port_ipv4(8443.tcp()).unwrap();
    let fake = Fake::start();
    fake.on_magnet(torrent(&files));
    fake.serve_files_from(&format!("https://localhost:{port}"));
    let dir = tempfile::tempdir().unwrap();

    // Without the CA the certificate must be rejected...
    let out = rd(
        &fake,
        dir.path(),
        &[],
        &["dl", &magnet(), "-o", "out", "-q", "-j", "1"],
    );
    assert!(!out.ok, "an untrusted certificate was accepted");
    assert!(out.stderr.contains("certificate"), "{}", out.stderr);
    assert!(!dir.path().join("out/tls.bin").exists());

    // ...and with RD_CA_CERT it downloads over TLS.
    let ca = dir.path().join("root.pem");
    std::fs::write(&ca, &root_pem).unwrap();
    let ca = ca.to_str().unwrap();
    let out = rd(
        &fake,
        dir.path(),
        &[("RD_CA_CERT", ca)],
        &["dl", &magnet(), "-o", "out", "-q"],
    );
    assert!(out.ok, "{}", out.stderr);
    assert_file(&dir.path().join("out/tls.bin"), &file);
}

#[test]
fn connections_cut_mid_chunk_resume_where_they_stopped() {
    let big = data(200 * MIB, 3);
    let files = [("cut.bin", big.clone())];
    let net = unique("net");
    let web = nginx(&files, Some(&net));
    let toxi = Toxi::start(&net, &format!("{}:80", web.name));
    // Every connection is closed after 8 MiB, so a 64 MiB chunk can only
    // finish if each retry continues where the last one was cut off.
    toxi.add_toxic(
        r#"{"name":"cut","type":"limit_data","stream":"downstream","toxicity":1.0,"attributes":{"bytes":8388608}}"#,
    );
    let fake = Fake::start();
    fake.on_magnet(torrent(&files));
    fake.serve_files_from(&toxi.url());
    let dir = tempfile::tempdir().unwrap();

    let out = rd(
        &fake,
        dir.path(),
        &[],
        &["dl", &magnet(), "-o", "out", "-q", "--concurrency", "4"],
    );
    assert!(out.ok, "{}", out.stderr);
    assert_file(&dir.path().join("out/cut.bin"), &big);
}

#[test]
fn stalled_server_times_out_then_resumes() {
    let big = data(300 * MIB, 4);
    let files = [("stall.bin", big.clone())];
    let net = unique("net");
    let web = nginx(&files, Some(&net));
    let toxi = Toxi::start(&net, &format!("{}:80", web.name));
    // ~10 MB/s so the download is still running when the stall starts.
    toxi.add_toxic(
        r#"{"name":"slow","type":"bandwidth","stream":"downstream","attributes":{"rate":10240}}"#,
    );
    let fake = Fake::start();
    fake.on_magnet(torrent(&files));
    fake.serve_files_from(&toxi.url());
    let dir = tempfile::tempdir().unwrap();

    let toxi_for_stall = &toxi;
    let started = Instant::now();
    let out = std::thread::scope(|s| {
        s.spawn(move || {
            std::thread::sleep(Duration::from_secs(3));
            // Stop sending data without closing anything.
            toxi_for_stall.add_toxic(
                r#"{"name":"stall","type":"timeout","stream":"downstream","attributes":{"timeout":0}}"#,
            );
        });
        rd(
            &fake,
            dir.path(),
            &[("RD_READ_TIMEOUT_SECS", "2")],
            &["dl", &magnet(), "-o", "out", "-q"],
        )
    });
    assert!(!out.ok, "a stalled download should fail, not hang");
    assert!(
        started.elapsed() < Duration::from_secs(90),
        "took {:?}",
        started.elapsed()
    );
    assert!(!dir.path().join("out/stall.bin.rdtmp").exists());

    toxi.remove_toxic("stall");
    toxi.remove_toxic("slow");
    let out = rd(
        &fake,
        dir.path(),
        &[],
        &["dl", &magnet(), "-o", "out", "-q"],
    );
    assert!(out.ok, "{}", out.stderr);
    assert_file(&dir.path().join("out/stall.bin"), &big);
}

#[test]
fn disk_too_small_fails_before_downloading() {
    let files = [("huge.bin", data(100 * MIB, 5))];
    let web = nginx(&files, None);
    let fake = Fake::start();
    fake.on_magnet(torrent(&files));
    fake.serve_files_from(&web.url());
    let rdbox = rd_box("64m");

    let out = rd_in_box(&rdbox, &fake, &["dl", &magnet(), "-o", "/out", "-q"]);
    assert_ne!(out.code, 0);
    assert!(
        out.stderr.contains("not enough disk space"),
        "{}",
        out.stderr
    );
    let left = exec(&rdbox, &["ls", "-A", "/out"]);
    assert_eq!(left.stdout.trim(), "", "nothing should be written");
}

#[test]
fn disk_filling_up_mid_download_fails_cleanly() {
    let (a, b) = (data(40 * MIB, 6), data(40 * MIB, 7));
    let files = [("a.bin", a.clone()), ("b.bin", b.clone())];
    let web = nginx(&files, None);
    let fake = Fake::start();
    fake.on_magnet(torrent(&files));
    fake.serve_files_from(&web.url());
    let rdbox = rd_box("64m");

    // Both files pass the free-space check on their own; together they
    // don't fit, so one hits ENOSPC (or the check, on the retry pass).
    let out = rd_in_box(
        &rdbox,
        &fake,
        &["dl", &magnet(), "-o", "/out", "-q", "-j", "2"],
    );
    assert_ne!(out.code, 0);
    assert!(
        out.stderr.contains("not enough disk space") || out.stderr.contains("No space left"),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("1 of 2 file(s) failed"),
        "{}",
        out.stderr
    );

    let left = exec(&rdbox, &["ls", "-A", "/out"]).stdout;
    let left: Vec<&str> = left.split_whitespace().collect();
    assert_eq!(
        left.len(),
        1,
        "exactly one finished file and no partials: {left:?}"
    );
    let expected = if left[0] == "a.bin" { &a } else { &b };
    let got = exec_bytes(&rdbox, &["cat", &format!("/out/{}", left[0])]);
    assert!(got == expected.as_slice(), "{} is corrupt", left[0]);
}
