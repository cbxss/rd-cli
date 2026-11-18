//! Downloading one file to disk: picking a safe destination, probing the
//! server, then either a plain stream (small files, no range support) or
//! resumable parallel chunks (large files).

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct DownloadOpts {
    /// Max parallel range requests per file.
    pub concurrency: usize,
    /// Suppress status lines and progress bars.
    pub quiet: bool,
}

/// What a download command achieved, for `--json` and the exit code.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    /// Files on disk (downloaded now, or already there from an earlier run).
    pub files: Vec<FileOut>,
    /// Links that couldn't be unrestricted or downloaded.
    pub failed: Vec<Failure>,
}

#[derive(Debug, Serialize)]
pub struct FileOut {
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct Failure {
    pub link: String,
    pub error: String,
    /// Whether rerunning the same command may fix it (a dropped or expired
    /// download, Real-Debrid busy), as opposed to e.g. a dead link or a
    /// full disk.
    pub retryable: bool,
}

/// Not enough free space for a download; rerunning won't help until space
/// is freed.
#[derive(Debug)]
pub struct NoSpace(String);

impl std::fmt::Display for NoSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NoSpace {}

/// See [`Failure::retryable`]. Real-Debrid errors count only when transient
/// (network, rate limiting, 5xx, too many active downloads); failures while
/// transferring a file count unless the disk is full.
pub fn is_retryable(err: &anyhow::Error) -> bool {
    use crate::api::error::RdError;
    if err.chain().any(|e| e.is::<NoSpace>()) {
        return false;
    }
    match err.chain().find_map(|e| e.downcast_ref::<RdError>()) {
        Some(RdError::Http(_) | RdError::RateLimited) => true,
        // 24 (unavailable_file) and 35 (infringing_file) come back as 503
        // but mean the file is gone; 21 is too_many_active_downloads, 34
        // too_many_requests.
        Some(RdError::Api { code: 24 | 35, .. }) => false,
        Some(RdError::Api { status, code, .. }) => *status >= 500 || matches!(code, 21 | 34),
        Some(_) => false,
        None => true,
    }
}

impl Report {
    pub fn add_file(&mut self, path: &Path) {
        let bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        self.files.push(FileOut {
            path: path.display().to_string(),
            bytes,
        });
    }

    pub fn add_failure(&mut self, link: &str, err: &anyhow::Error) {
        self.failed.push(Failure {
            link: link.to_string(),
            error: format!("{err:#}"),
            retryable: is_retryable(err),
        });
    }

    /// Prints the JSON summary if asked, then fails if anything failed (each
    /// failure was already reported on stderr): with [`Incomplete`] when a
    /// rerun may fix at least one of them.
    pub fn finish(self, json: bool) -> Result<()> {
        if json {
            crate::output::print_json(&self)?;
        }
        if self.failed.is_empty() {
            return Ok(());
        }
        let total = self.files.len() + self.failed.len();
        let msg = format!("{} of {total} file(s) failed", self.failed.len());
        if self.failed.iter().any(|f| f.retryable) {
            Err(Incomplete(format!(
                "{msg}; rerun the same command to retry them (finished files are skipped)"
            ))
            .into())
        } else {
            Err(anyhow::anyhow!(msg))
        }
    }
}

/// Marks a failure that rerunning the same command may fix: downloads that
/// failed in a retryable way (finished files are skipped and partial ones
/// resume on a rerun) or a torrent that stopped making progress. `main` exits with
/// [`Incomplete::EXIT_CODE`] for it.
#[derive(Debug)]
pub struct Incomplete(pub String);

impl Incomplete {
    pub const EXIT_CODE: i32 = 3;
}

impl std::fmt::Display for Incomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Incomplete {}

/// Files at least this big use the chunked path, which is resumable (even
/// with one connection); smaller ones are fetched in one stream.
const SEGMENT_THRESHOLD: u64 = 50 * 1024 * 1024;

/// Size of each range request in the chunked path. Fixed-size chunks (rather
/// than one segment per connection) keep a failed attempt and an interrupted
/// run cheap — at most one chunk per connection is lost, which matters for
/// multi-hundred-GB files — and resume works regardless of `--concurrency`.
const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Attempts for a probe or a whole single-stream download; for a chunk, the
/// number of consecutive attempts that make no progress.
const MAX_ATTEMPTS: u32 = 5;

/// Longest file name we create, in bytes. Most filesystems allow 255; the
/// rest is headroom for ` (n)` and the `.rdpart.tmp` / `.rdtmp` suffixes.
const MAX_NAME_BYTES: usize = 200;

// ---- shared process state ----

/// One HTTP client for every download in the process, so connections (and
/// TLS sessions) to the same download server are reused across files.
fn http() -> Result<&'static Client> {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    if let Some(c) = CLIENT.get() {
        return Ok(c);
    }
    let client = crate::http::builder()?
        .build()
        .context("failed to build HTTP client")?;
    Ok(CLIENT.get_or_init(|| client))
}

/// All progress bars live here, so several files can download at once
/// without their bars overwriting each other.
fn bars() -> &'static MultiProgress {
    static BARS: OnceLock<MultiProgress> = OnceLock::new();
    BARS.get_or_init(MultiProgress::new)
}

/// Prints a status line to stdout without garbling active progress bars.
pub fn status(msg: &str) {
    bars().suspend(|| println!("{msg}"));
}

/// Paths being written by this process right now. Two downloads must never
/// pick the same file (or its `.rdpart` / `.rdtmp`), whatever their names.
fn in_use() -> &'static Mutex<HashSet<PathBuf>> {
    static IN_USE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    IN_USE.get_or_init(Default::default)
}

/// A path reserved in [`in_use`] until dropped.
struct Claim(PathBuf);

impl Drop for Claim {
    fn drop(&mut self) {
        in_use()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.0);
    }
}

// ---- errors and retries ----

/// Failures that retrying the same request won't fix.
#[derive(Debug, thiserror::Error)]
enum Permanent {
    /// E.g. an expired link (403/404).
    #[error("{0}")]
    Fatal(String),
    /// The server answered a range request with something else; the file
    /// can still be fetched in one stream.
    #[error("{0}")]
    RangesIgnored(String),
}

fn permanent(err: &anyhow::Error) -> Option<&Permanent> {
    err.chain().find_map(|e| e.downcast_ref::<Permanent>())
}

fn is_permanent(err: &anyhow::Error) -> bool {
    permanent(err).is_some()
}

/// Like `error_for_status`, but 4xx responses (other than timeouts and rate
/// limits) are [`Permanent::Fatal`].
fn check_status(resp: Response, what: &str) -> Result<Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let msg = format!("{what}: server returned HTTP {status}");
    if status.is_client_error()
        && status != StatusCode::REQUEST_TIMEOUT
        && status != StatusCode::TOO_MANY_REQUESTS
    {
        return Err(Permanent::Fatal(msg).into());
    }
    Err(anyhow::anyhow!(msg))
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(200 * 2u64.pow(attempt.saturating_sub(1).min(6)))
}

/// Runs `attempt` up to [`MAX_ATTEMPTS`] times, backing off between
/// transient failures.
async fn retrying<T, F, Fut>(mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut n = 0;
    loop {
        n += 1;
        match attempt().await {
            Ok(v) => return Ok(v),
            Err(e) if is_permanent(&e) || n >= MAX_ATTEMPTS => return Err(e),
            Err(_) => tokio::time::sleep(retry_delay(n)).await,
        }
    }
}

// ---- disk space ----

/// Fails before any data is fetched when the destination's filesystem can't
/// hold the `needed` bytes still to download. If free space can't be
/// determined, the download goes ahead.
fn ensure_space(dest: &Path, needed: u64) -> Result<()> {
    let dir = parent_dir(dest);
    match fs4::available_space(dir) {
        Ok(free) => check_space(needed, free, dir),
        Err(_) => Ok(()),
    }
}

fn check_space(needed: u64, free: u64, dir: &Path) -> Result<()> {
    if needed > free {
        return Err(NoSpace(format!(
            "not enough disk space in {}: need {}, only {} free",
            dir.display(),
            crate::output::human_bytes(needed as i64),
            crate::output::human_bytes(free as i64),
        ))
        .into());
    }
    Ok(())
}

/// Bytes the filesystem has actually allocated to `path` (preallocation is
/// sparse on most Unix filesystems but not on NTFS).
fn allocated_bytes(path: &Path) -> u64 {
    let Ok(meta) = std::fs::metadata(path) else {
        return 0;
    };
    #[cfg(unix)]
    {
        std::os::unix::fs::MetadataExt::blocks(&meta) * 512
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

/// Creates `-o`'s directory if one was given; defaults to the current one.
pub fn prepare_out_dir(output_dir: Option<&str>) -> Result<PathBuf> {
    let Some(dir) = output_dir else {
        return Ok(PathBuf::from("."));
    };
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create output directory {dir}"))?;
    Ok(PathBuf::from(dir))
}

// ---- names ----

/// Reduces a server-supplied file name to one portable path component, so it
/// can't escape the output directory or misbehave on any OS: path separators
/// and characters Windows forbids (`<>:"|?*`, control characters) become `_`
/// (`C:x` would otherwise be a drive-relative path, `a:b` an NTFS stream),
/// trailing dots and spaces go, reserved device names (`CON`, `NUL.txt`,
/// ...) get a `_` prefix, and over-long names are shortened.
pub fn safe_filename(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = base
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '|' | '?' | '*' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let cleaned = cleaned.trim().trim_end_matches(['.', ' ']).to_string();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        return "download".to_string();
    }
    let stem = cleaned.split('.').next().unwrap_or("").to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && stem.as_bytes()[3].is_ascii_digit());
    let named = if reserved {
        format!("_{cleaned}")
    } else {
        cleaned
    };
    shorten(&named, MAX_NAME_BYTES)
}

/// Cuts `name` to at most `max` bytes on a character boundary, keeping a
/// short extension.
fn shorten(name: &str, max: usize) -> String {
    if name.len() <= max {
        return name.to_string();
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() && e.len() <= 16 => (s, format!(".{e}")),
        _ => (name, String::new()),
    };
    let mut cut = max - ext.len();
    while !stem.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{ext}", &stem[..cut])
}

/// `name` for n = 1, else `stem (n).ext`.
fn numbered_name(name: &str, n: u32) -> String {
    if n == 1 {
        return name.to_string();
    }
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => format!("{stem} ({n}).{ext}"),
        _ => format!("{name} ({n})"),
    }
}

/// The first of `name`, `name (2).ext`, ... not yet in `claimed`. Compared
/// case-insensitively, since Windows and macOS filesystems are.
pub fn unique_name(claimed: &mut HashSet<String>, name: &str) -> String {
    (1..)
        .map(|n| numbered_name(name, n))
        .find(|candidate| claimed.insert(candidate.to_lowercase()))
        .expect("unbounded")
}

/// Real-Debrid reports how many parallel connections ("chunks") a link allows;
/// opening more risks refused or throttled requests.
pub fn cap_connections(requested: usize, allowed: i64) -> usize {
    if allowed > 0 {
        requested.min(allowed as usize).max(1)
    } else {
        requested.max(1)
    }
}

// ---- resume state ----

/// `<dest>.rdpart`: which chunks of a chunked download are on disk.
#[derive(Debug, Serialize, Deserialize)]
struct Sidecar {
    total_size: u64,
    completed_ranges: Vec<(u64, u64)>,
}

impl Sidecar {
    fn path_for(dest: &Path) -> PathBuf {
        PathBuf::from(format!("{}.rdpart", dest.display()))
    }

    /// Loads the sidecar for `dest` if it describes a file of `total_size`
    /// bytes. The download URL isn't part of it: Real-Debrid can hand out a
    /// different URL each time a link is unrestricted.
    fn load(dest: &Path, total_size: u64) -> Option<Sidecar> {
        let data = std::fs::read(Self::path_for(dest)).ok()?;
        let sidecar: Sidecar = serde_json::from_slice(&data).ok()?;
        (sidecar.total_size == total_size).then_some(sidecar)
    }

    /// Records a finished range, merging it with adjacent ones so the sidecar
    /// stays small even for thousands of chunks.
    fn mark_done(&mut self, range: (u64, u64)) {
        self.completed_ranges.push(range);
        self.completed_ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.completed_ranges.len());
        for &(s, e) in &self.completed_ranges {
            match merged.last_mut() {
                Some(last) if s <= last.1.saturating_add(1) => last.1 = last.1.max(e),
                _ => merged.push((s, e)),
            }
        }
        self.completed_ranges = merged;
    }

    fn covers(&self, (s, e): (u64, u64)) -> bool {
        self.completed_ranges
            .iter()
            .any(|&(cs, ce)| cs <= s && e <= ce)
    }

    /// Write-then-rename, with the temp file synced first, so after a crash or
    /// power cut the sidecar is the old or the new version, never empty.
    fn save(&self, dest: &Path) -> Result<()> {
        let path = Self::path_for(dest);
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));
        let write = || -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, &serde_json::to_vec(self)?)?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            #[cfg(unix)]
            std::fs::File::open(parent_dir(&path))?.sync_all()?;
            Ok(())
        };
        write().with_context(|| format!("failed to save {}", path.display()))
    }

    fn delete(dest: &Path) {
        let _ = std::fs::remove_file(Self::path_for(dest));
    }
}

// ---- choosing the destination ----

enum Picked {
    /// A finished earlier download of this file.
    Finished(PathBuf),
    /// Where to write (resuming a partial download of ours if there is one).
    Write(Claim),
}

/// Decides where a download of `total` bytes goes, trying `dest`, then
/// `name (2).ext`, ...:
/// - a path another download in this run is using is skipped;
/// - a `.rdpart` for the same size is our partial download: resume it;
///   one for another size belongs to a different file and is left alone;
/// - a file of exactly `total` bytes (and no `.rdpart`) is a finished
///   earlier download;
/// - any other existing file is someone else's and is never overwritten.
fn pick_destination(dest: &Path, total: Option<u64>) -> Result<Picked> {
    let mut in_use = in_use().lock().unwrap_or_else(|p| p.into_inner());
    let name = dest
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    for n in 1..=1000 {
        let candidate = dest.with_file_name(numbered_name(&name, n));
        if in_use.contains(&candidate) {
            continue;
        }
        if Sidecar::path_for(&candidate).exists() {
            if total.is_some_and(|t| Sidecar::load(&candidate, t).is_some()) {
                in_use.insert(candidate.clone());
                return Ok(Picked::Write(Claim(candidate)));
            }
            continue;
        }
        match std::fs::metadata(&candidate) {
            Err(_) => {
                in_use.insert(candidate.clone());
                return Ok(Picked::Write(Claim(candidate)));
            }
            Ok(m) if m.is_file() && Some(m.len()) == total => {
                return Ok(Picked::Finished(candidate));
            }
            Ok(_) => continue,
        }
    }
    anyhow::bail!("no free file name for {}", dest.display())
}

// ---- downloading ----

/// Result of probing a URL for range support.
struct ProbeResult {
    supports_ranges: bool,
    total_size: Option<u64>,
}

/// Asks for the first byte. A `206` both proves range support and reveals the
/// size; anything else means a plain single-stream download. Servers that
/// merely advertise `Accept-Ranges` aren't trusted.
async fn probe(client: &Client, url: &str) -> Result<ProbeResult> {
    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()
        .await
        .context("failed to probe URL for range support")?;
    let resp = check_status(resp, "probe failed")?;

    if resp.status() == StatusCode::PARTIAL_CONTENT {
        // Content-Range: bytes 0-0/12345
        let total_size = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next())
            .and_then(|v| v.trim().parse::<u64>().ok());
        return Ok(ProbeResult {
            supports_ranges: total_size.is_some(),
            total_size,
        });
    }

    Ok(ProbeResult {
        supports_ranges: false,
        total_size: resp.content_length(),
    })
}

/// Downloads `url` (shown as `label`) to `dest`, or to a numbered variant
/// if a different file already has that name, and returns the path written.
/// `chunks` is Real-Debrid's per-link connection limit (0 if unknown).
pub async fn fetch_file(
    url: &str,
    label: &str,
    chunks: i64,
    dest: &Path,
    opts: DownloadOpts,
) -> Result<PathBuf> {
    let opts = DownloadOpts {
        concurrency: cap_connections(opts.concurrency, chunks),
        ..opts
    };
    fetch(url, label, dest, opts)
        .await
        .with_context(|| format!("failed to download {label}"))
}

async fn fetch(url: &str, label: &str, dest: &Path, opts: DownloadOpts) -> Result<PathBuf> {
    let client = http()?;
    let probed = retrying(|| probe(client, url)).await?;

    let claim = match pick_destination(dest, probed.total_size)? {
        Picked::Finished(path) => {
            if !opts.quiet {
                status(&format!("already downloaded: {}", path.display()));
            }
            return Ok(path);
        }
        Picked::Write(claim) => claim,
    };
    let path = claim.0.clone();
    if !opts.quiet {
        if path != dest {
            status(&format!(
                "{} already exists with different content; saving as {}",
                dest.display(),
                path.display()
            ));
        }
        status(&format!("downloading {label} -> {}", path.display()));
    }

    let chunked_size = probed
        .total_size
        .filter(|&size| probed.supports_ranges && size >= SEGMENT_THRESHOLD);
    let Some(total) = chunked_size else {
        fetch_single_stream(client, url, &path, probed.total_size, opts).await?;
        if !opts.quiet {
            status(&format!("finished: {}", path.display()));
        }
        return Ok(path);
    };
    match fetch_segmented(client, url, &path, total, opts).await {
        Err(e) if matches!(permanent(&e), Some(Permanent::RangesIgnored(_))) => {
            if !opts.quiet {
                status(&format!(
                    "the server doesn't honour range requests; downloading {label} in one stream"
                ));
            }
            // The preallocated file and its sidecar are of no use now.
            let _ = std::fs::remove_file(&path);
            Sidecar::delete(&path);
            fetch_single_stream(client, url, &path, Some(total), opts).await?;
        }
        other => other?,
    }
    drop(claim);
    if !opts.quiet {
        status(&format!("finished: {}", path.display()));
    }
    Ok(path)
}

/// A progress bar, plus — when bars can't be drawn (output redirected to a
/// log, `nohup`, an agent capturing output) — a ticker printing a plain
/// progress line every `RD_PROGRESS_MS` (default 30s). The ticker stops when
/// this is dropped, on every exit path.
struct Progress {
    pb: ProgressBar,
    ticker: Option<tokio::task::JoinHandle<()>>,
}

impl std::ops::Deref for Progress {
    type Target = ProgressBar;
    fn deref(&self) -> &ProgressBar {
        &self.pb
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        if let Some(t) = self.ticker.take() {
            t.abort();
        }
    }
}

fn plain_progress_interval() -> Duration {
    std::env::var("RD_PROGRESS_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(30))
}

/// `47s`, `12m`, `3h05m`.
fn short_duration(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
    }
}

/// One plain progress line. `rate` is bytes/second over the last interval.
fn plain_progress_line(label: &str, done: u64, total: Option<u64>, rate: f64) -> String {
    use crate::output::human_bytes;
    let speed = format!("{}/s", human_bytes(rate as i64));
    match total.filter(|&t| t > 0) {
        Some(total) => {
            let pct = done as f64 * 100.0 / total as f64;
            let eta = if rate >= 1.0 {
                short_duration((total.saturating_sub(done) as f64 / rate) as u64)
            } else {
                "?".to_string()
            };
            format!(
                "progress: {label}: {} / {} ({pct:.1}%), {speed}, ETA {eta}",
                human_bytes(done as i64),
                human_bytes(total as i64)
            )
        }
        None => format!(
            "progress: {label}: {} so far, {speed}",
            human_bytes(done as i64)
        ),
    }
}

fn spawn_plain_progress(pb: ProgressBar, label: String) -> tokio::task::JoinHandle<()> {
    let interval = plain_progress_interval();
    tokio::spawn(async move {
        let mut last = (Instant::now(), pb.position());
        loop {
            tokio::time::sleep(interval).await;
            let now = (Instant::now(), pb.position());
            let secs = now.0.duration_since(last.0).as_secs_f64();
            let rate = now.1.saturating_sub(last.1) as f64 / secs.max(0.001);
            last = now;
            status(&plain_progress_line(&label, now.1, pb.length(), rate));
        }
    })
}

/// Creates the bar for downloading to `dest`, already at `start` bytes (so
/// resumed bytes don't count towards the speed).
fn make_progress_bar(total: Option<u64>, quiet: bool, dest: &Path, start: u64) -> Progress {
    if quiet {
        return Progress {
            pb: ProgressBar::hidden(),
            ticker: None,
        };
    }
    let pb = match total {
        Some(total) => {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})",
                )
                .unwrap()
                .progress_chars("#>-"),
            );
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] {bytes} downloaded ({bytes_per_sec})",
                )
                .unwrap(),
            );
            pb
        }
    };
    pb.set_position(start);
    let pb = bars().add(pb);
    let ticker = bars().is_hidden().then(|| {
        let label = dest
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        spawn_plain_progress(pb.clone(), label)
    });
    Progress { pb, ticker }
}

/// Plain GET for small files and servers without range support. Writes to
/// `<dest>.rdtmp` and renames on success, so an interrupted download never
/// leaves a truncated file under the real name; transient failures restart
/// the transfer.
async fn fetch_single_stream(
    client: &Client,
    url: &str,
    dest: &Path,
    total_size: Option<u64>,
    opts: DownloadOpts,
) -> Result<()> {
    if let Some(total) = total_size {
        ensure_space(dest, total)?;
    }
    let tmp = PathBuf::from(format!("{}.rdtmp", dest.display()));
    let pb = make_progress_bar(total_size, opts.quiet, dest, 0);
    let result = retrying(|| {
        pb.set_position(0);
        single_stream_once(client, url, &tmp, total_size, &pb)
    })
    .await;
    pb.finish_and_clear();

    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, dest)
        .with_context(|| format!("failed to move download into place at {}", dest.display()))?;
    // A sidecar left by an earlier chunked attempt would make the finished
    // file look partial forever.
    Sidecar::delete(dest);
    Ok(())
}

async fn single_stream_once(
    client: &Client,
    url: &str,
    tmp: &Path,
    probed_size: Option<u64>,
    pb: &ProgressBar,
) -> Result<()> {
    let resp = client.get(url).send().await.context("request failed")?;
    let resp = check_status(resp, "download failed")?;
    // Without a Content-Length (chunked or close-delimited bodies) a dropped
    // connection looks like a normal end, so fall back to the probed size.
    let expected = resp.content_length().or(probed_size);
    if let Some(len) = expected {
        pb.set_length(len);
    }

    let mut file = tokio::fs::File::create(tmp)
        .await
        .with_context(|| format!("failed to create {}", tmp.display()))?;
    let mut written = 0u64;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error while streaming response body")?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .context("failed to write to destination file")?;
        written += chunk.len() as u64;
        pb.inc(chunk.len() as u64);
    }
    tokio::io::AsyncWriteExt::flush(&mut file).await?;

    if let Some(len) = expected.filter(|&len| len != written) {
        anyhow::bail!("short read: expected {len} bytes, got {written}");
    }
    // Durable before the rename publishes it under the real name.
    file.sync_data()
        .await
        .context("failed to flush download to disk")?;
    Ok(())
}

/// Splits `[0, total_size)` into inclusive byte ranges of at most `chunk` bytes.
fn split_ranges(total_size: u64, chunk: u64) -> Vec<(u64, u64)> {
    let chunk = chunk.max(1);
    let mut ranges = Vec::new();
    let mut start = 0u64;
    while start < total_size {
        let end = start.saturating_add(chunk - 1).min(total_size - 1);
        ranges.push((start, end));
        start = end + 1;
    }
    ranges
}

async fn fetch_segmented(
    client: &Client,
    url: &str,
    dest: &Path,
    total_size: u64,
    opts: DownloadOpts,
) -> Result<()> {
    let all_ranges = split_ranges(total_size, CHUNK_SIZE);

    let existing_sidecar = Sidecar::load(dest, total_size).filter(|_| dest.exists());
    let resuming = existing_sidecar.is_some();
    let sidecar = Sidecar {
        total_size,
        completed_ranges: existing_sidecar
            .map(|s| s.completed_ranges)
            .unwrap_or_default(),
    };

    type Ranges = Vec<(u64, u64)>;
    let (done_ranges, pending_ranges): (Ranges, Ranges) =
        all_ranges.into_iter().partition(|r| sidecar.covers(*r));
    let already_done_bytes: u64 = done_ranges.iter().map(|(s, e)| e - s + 1).sum();

    // Check before preallocating: on many filesystems the preallocation is
    // sparse, so a full disk would otherwise only show up hours in. Space the
    // partial file already occupies (all of it, on NTFS) isn't needed again.
    let occupied = if resuming { allocated_bytes(dest) } else { 0 };
    ensure_space(
        dest,
        total_size - already_done_bytes.max(occupied).min(total_size),
    )?;

    // Save the sidecar before creating the file: a full-size file without
    // one would look finished to the "already downloaded" check. Then
    // preallocate (keeping what's there when resuming).
    sidecar.save(dest)?;
    {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(!resuming)
            .open(dest)
            .with_context(|| format!("failed to create {}", dest.display()))?;
        file.set_len(total_size)
            .context("failed to preallocate destination file")?;
    }

    let sidecar = Arc::new(tokio::sync::Mutex::new(sidecar));
    let bytes_done = Arc::new(AtomicU64::new(already_done_bytes));

    if resuming && !opts.quiet {
        status(&format!(
            "resuming: {} of {} already downloaded",
            crate::output::human_bytes(already_done_bytes as i64),
            crate::output::human_bytes(total_size as i64)
        ));
    }

    let pb = make_progress_bar(Some(total_size), opts.quiet, dest, already_done_bytes);

    let mut tasks = tokio::task::JoinSet::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(opts.concurrency.max(1)));
    // Set by the first chunk that fails, so queued chunks don't start new
    // requests in the moment before the remaining tasks are aborted.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    for range in pending_ranges {
        let client = client.clone();
        let url = url.to_string();
        let dest = dest.to_path_buf();
        let bytes_done = Arc::clone(&bytes_done);
        let pb = pb.clone();
        let sem = Arc::clone(&sem);
        let sidecar = Arc::clone(&sidecar);
        let stop = Arc::clone(&stop);

        tasks.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            if stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            if let Err(e) = download_segment(&client, &url, &dest, range, &bytes_done, &pb).await {
                stop.store(true, Ordering::SeqCst);
                return Err(e);
            }
            {
                let mut sc = sidecar.lock().await;
                sc.mark_done(range);
                sc.save(&dest)?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    // Stop at the first chunk that fails for good: with thousands of chunks,
    // carrying on would retry a dead link for every one of them. Finished
    // chunks are already in the sidecar, so a rerun picks up from here.
    let mut first_error: Option<anyhow::Error> = None;
    while let Some(res) = tasks.join_next().await {
        let err = match res {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => e,
            Err(join_err) if join_err.is_cancelled() => continue,
            Err(join_err) => anyhow::anyhow!("segment task panicked: {join_err}"),
        };
        if first_error.is_none() {
            first_error = Some(err);
            tasks.abort_all();
        }
    }

    pb.finish_and_clear();

    if let Some(e) = first_error {
        // Make the way out obvious: the full-size file on disk looks finished
        // (or corrupt) but isn't, and deleting it throws the progress away.
        return Err(e.context(format!(
            "partial download kept: {} is incomplete (progress in {}); rerun the same command to resume, and don't delete either file",
            dest.display(),
            Sidecar::path_for(dest).display()
        )));
    }

    Sidecar::delete(dest);

    Ok(())
}

/// Downloads one chunk. A cut-off attempt keeps what it wrote: the retry
/// asks only for the rest of the chunk. Gives up after `MAX_ATTEMPTS`
/// consecutive attempts that made no progress.
async fn download_segment(
    client: &Client,
    url: &str,
    dest: &Path,
    range: (u64, u64),
    bytes_done: &Arc<AtomicU64>,
    pb: &ProgressBar,
) -> Result<()> {
    let (start, end) = range;
    let file = Arc::new(
        std::fs::OpenOptions::new()
            .write(true)
            .open(dest)
            .with_context(|| format!("failed to open {} for writing", dest.display()))?,
    );

    let mut written = 0u64;
    let mut stalled_attempts = 0u32;
    loop {
        // An attempt can deliver every byte and still fail (the connection
        // drops before the body is formally complete); then there's nothing
        // left to request.
        let result = if start + written > end {
            Ok(())
        } else {
            let (progress, result) =
                download_range_once(client, url, &file, (start + written, end), bytes_done, pb)
                    .await;
            written += progress;
            if result.is_err() {
                stalled_attempts = if progress > 0 {
                    1
                } else {
                    stalled_attempts + 1
                };
            }
            result
        };
        match result {
            Ok(()) => {
                // Make the chunk durable before the caller records it as done
                // in the sidecar; otherwise a power cut could leave a sidecar
                // vouching for data that never reached the disk.
                let file = Arc::clone(&file);
                tokio::task::spawn_blocking(move || file.sync_data())
                    .await
                    .context("blocking sync task panicked")?
                    .context("failed to flush chunk to disk")?;
                return Ok(());
            }
            Err(e) if is_permanent(&e) => return Err(e),
            Err(_) if start + written > end => continue,
            Err(e) => {
                if stalled_attempts >= MAX_ATTEMPTS {
                    return Err(e.context(format!(
                        "failed to download byte range {start}-{end} after {MAX_ATTEMPTS} attempts without progress"
                    )));
                }
                tokio::time::sleep(retry_delay(stalled_attempts)).await;
            }
        }
    }
}

/// Requests `range` once and writes it in place. Returns how many bytes were
/// written (kept even if the attempt then fails) and the outcome.
async fn download_range_once(
    client: &Client,
    url: &str,
    file: &Arc<std::fs::File>,
    range: (u64, u64),
    bytes_done: &AtomicU64,
    pb: &ProgressBar,
) -> (u64, Result<()>) {
    let (start, end) = range;
    let expected = end - start + 1;
    let mut written = 0u64;

    let result: Result<()> = async {
        let resp = client
            .get(url)
            .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
            .send()
            .await
            .with_context(|| format!("request failed for range {start}-{end}"))?;
        let resp = check_status(resp, &format!("range {start}-{end}"))?;

        // Writing a non-ranged (200) body or a different range at `start`
        // would silently corrupt the file, so insist on exactly this range.
        if resp.status() != StatusCode::PARTIAL_CONTENT {
            return Err(Permanent::RangesIgnored(format!(
                "server ignored the range request for {start}-{end} (HTTP {})",
                resp.status()
            ))
            .into());
        }
        let served = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range);
        if served != Some((start, end)) {
            return Err(Permanent::RangesIgnored(format!(
                "server returned the wrong range for {start}-{end} ({served:?})"
            ))
            .into());
        }

        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.with_context(|| {
                format!("error while streaming response body for range {start}-{end}")
            })?;
            let len = chunk.len() as u64;
            if written + len > expected {
                anyhow::bail!("server sent more than the requested range {start}-{end}");
            }
            let offset = start + written;
            let file = Arc::clone(file);
            tokio::task::spawn_blocking(move || write_all_at(&file, &chunk, offset))
                .await
                .context("blocking write task panicked")?
                .context("positioned write failed")?;
            written += len;
            pb.set_position(bytes_done.fetch_add(len, Ordering::Relaxed) + len);
        }
        if written != expected {
            anyhow::bail!(
                "short read for range {start}-{end}: expected {expected} bytes, got {written}"
            );
        }
        Ok(())
    }
    .await;

    (written, result)
}

/// Parses `Content-Range: bytes S-E/TOTAL` into `(S, E)`.
fn parse_content_range(v: &str) -> Option<(u64, u64)> {
    let range = v.trim().strip_prefix("bytes ")?.split('/').next()?;
    let (s, e) = range.split_once('-')?;
    Some((s.trim().parse().ok()?, e.trim().parse().ok()?))
}

#[cfg(unix)]
fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let written = file.write_at(buf, offset)?;
        if written == 0 {
            anyhow::bail!("positioned write returned 0 bytes");
        }
        offset += written as u64;
        buf = &buf[written..];
    }

    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let written = file.seek_write(buf, offset)?;
        if written == 0 {
            anyhow::bail!("positioned write returned 0 bytes");
        }
        offset += written as u64;
        buf = &buf[written..];
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_failures() {
        use crate::api::error::RdError;
        let full = check_space(10, 1, Path::new(".")).unwrap_err();
        assert!(!is_retryable(&full.context("failed to download x")));
        let dead: anyhow::Error = RdError::Api {
            code: 24,
            message: "unavailable_file".into(),
            status: 503,
        }
        .into();
        assert!(!is_retryable(&dead), "a missing file stays missing");
        let busy: anyhow::Error = RdError::Api {
            code: 25,
            message: "service_unavailable".into(),
            status: 503,
        }
        .into();
        assert!(is_retryable(&busy), "other 5xx are transient");
        let gone: anyhow::Error = RdError::Api {
            code: 19,
            message: "hoster_unavailable".into(),
            status: 400,
        }
        .into();
        assert!(!is_retryable(&gone));
        assert!(is_retryable(&anyhow::anyhow!("short read")));
    }

    #[test]
    fn split_ranges_uses_fixed_chunks() {
        assert_eq!(split_ranges(0, 10), vec![]);
        assert_eq!(split_ranges(25, 10), vec![(0, 9), (10, 19), (20, 24)]);
        let big = 373 * 1024 * 1024 * 1024u64;
        let r = split_ranges(big, CHUNK_SIZE);
        assert_eq!(r.len() as u64, big.div_ceil(CHUNK_SIZE));
        assert_eq!(r.last().unwrap().1, big - 1);
    }

    #[test]
    fn sidecar_merges_and_covers() {
        let mut sc = Sidecar {
            total_size: 30,
            completed_ranges: vec![],
        };
        sc.mark_done((10, 19));
        sc.mark_done((0, 9));
        sc.mark_done((25, 29));
        assert_eq!(sc.completed_ranges, vec![(0, 19), (25, 29)]);
        assert!(sc.covers((0, 9)));
        assert!(sc.covers((5, 15)));
        assert!(!sc.covers((15, 25)));
    }

    async fn range_ignoring_server(data: Vec<u8>) -> wiremock::MockServer {
        use wiremock::matchers::{header, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // The probe gets a proper 206, every real range request gets the whole body.
        Mock::given(method("GET"))
            .and(header("range", "bytes=0-0"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", format!("bytes 0-0/{}", data.len()))
                    .set_body_bytes(vec![data[0]]),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(data))
            .mount(&server)
            .await;
        server
    }

    fn temp_dest() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        (dir, dest)
    }

    #[tokio::test]
    async fn server_ignoring_ranges_falls_back_to_one_stream() {
        let total = (SEGMENT_THRESHOLD + 10) as usize;
        let data: Vec<u8> = (0..total).map(|i| (i % 249) as u8).collect();
        let server = range_ignoring_server(data.clone()).await;
        let (_dir, dest) = temp_dest();
        let opts = DownloadOpts {
            concurrency: 2,
            quiet: true,
        };
        let path = fetch(&server.uri(), "out.bin", &dest, opts).await.unwrap();
        assert_eq!(path, dest);
        assert!(std::fs::read(&dest).unwrap() == data);
        assert!(!Sidecar::path_for(&dest).exists());
    }

    #[tokio::test]
    async fn finished_file_is_not_downloaded_again() {
        let data = vec![1u8; 1000];
        let server = range_ignoring_server(data.clone()).await;
        let (_dir, dest) = temp_dest();
        std::fs::write(&dest, &data).unwrap();
        let opts = DownloadOpts {
            concurrency: 1,
            quiet: true,
        };
        fetch(&server.uri(), "out.bin", &dest, opts).await.unwrap();
        let requests = server.received_requests().await.unwrap().len();
        assert_eq!(requests, 1, "only the probe should hit the server");
    }

    #[test]
    fn space_check() {
        let dir = Path::new("/data");
        assert!(check_space(10, 10, dir).is_ok());
        let err = check_space(400 << 30, 100 << 30, dir)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("need 400.00 GiB, only 100.00 GiB free"),
            "{err}"
        );
        // A real directory reports some free space.
        assert!(fs4::available_space(std::env::temp_dir()).unwrap() > 0);
    }

    #[test]
    fn content_range_parsing() {
        assert_eq!(parse_content_range("bytes 0-99/1000"), Some((0, 99)));
        assert_eq!(parse_content_range("bytes */1000"), None);
    }

    /// A one-shot raw HTTP server: answers each connection with
    /// `respond(request_head)` and then closes it.
    async fn raw_server(respond: impl Fn(&str) -> Vec<u8> + Send + Sync + 'static) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let respond = Arc::new(respond);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let respond = Arc::clone(&respond);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    let _ = sock.write_all(&respond(&head)).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        format!("http://{addr}/f.bin")
    }

    fn requested_range(head: &str) -> Option<(usize, usize)> {
        let line = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("range:"))?;
        let (s, e) = line.split_once('=')?.1.trim().split_once('-')?;
        Some((s.parse().ok()?, e.parse().ok()?))
    }

    #[tokio::test]
    async fn chunk_fully_received_before_a_dropped_connection_is_kept() {
        // Chunked range responses whose connection closes after the last data
        // byte but before the end-of-body marker.
        let total = (SEGMENT_THRESHOLD + 4096) as usize;
        let data: Arc<Vec<u8>> = Arc::new((0..total).map(|i| (i % 253) as u8).collect());
        let d = Arc::clone(&data);
        let url = raw_server(move |head| {
            let (s, e) = requested_range(head).unwrap();
            let mut out = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {s}-{e}/{}\r\n\
                 Transfer-Encoding: chunked\r\n\r\n{:x}\r\n",
                d.len(),
                e - s + 1
            )
            .into_bytes();
            out.extend_from_slice(&d[s..=e]);
            out.extend_from_slice(b"\r\n"); // ...and no terminating 0-chunk
            out
        })
        .await;
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path().to_path_buf();
        let dest = dir.join("f.bin");
        let opts = DownloadOpts {
            concurrency: 2,
            quiet: true,
        };
        let res = fetch(&url, "f.bin", &dest, opts).await;
        let got = std::fs::read(&dest).ok();
        std::fs::remove_dir_all(&dir).unwrap();
        res.unwrap();
        assert!(got.as_deref() == Some(&data[..]));
    }

    #[tokio::test]
    async fn truncated_body_without_content_length_is_an_error() {
        // The probe says 1000 bytes; the GET is a close-delimited 200 that
        // stops after 500.
        let url = raw_server(|head| {
            if requested_range(head).is_some() {
                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-0/1000\r\nContent-Length: 1\r\n\r\nx".to_vec()
            } else {
                let mut out = b"HTTP/1.0 200 OK\r\n\r\n".to_vec();
                out.extend_from_slice(&[7u8; 500]);
                out
            }
        })
        .await;
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path().to_path_buf();
        let dest = dir.join("f.bin");
        let opts = DownloadOpts {
            concurrency: 1,
            quiet: true,
        };
        let res = fetch(&url, "f.bin", &dest, opts).await;
        let exists = dest.exists();
        std::fs::remove_dir_all(&dir).unwrap();
        let err = res.unwrap_err();
        assert!(format!("{err:#}").contains("short read"), "{err:#}");
        assert!(!exists, "a truncated file was published");
    }

    #[test]
    fn safe_filename_handles_windows_names() {
        assert_eq!(safe_filename("C:evil.exe"), "C_evil.exe");
        assert_eq!(safe_filename("a:b.mkv"), "a_b.mkv");
        assert_eq!(safe_filename("what?.mkv"), "what_.mkv");
        assert_eq!(safe_filename("NUL.txt"), "_NUL.txt");
        assert_eq!(safe_filename("com1"), "_com1");
        assert_eq!(safe_filename("console.txt"), "console.txt");
        assert_eq!(safe_filename("name. "), "name");
        assert_eq!(safe_filename("tab\there"), "tab_here");
    }

    #[test]
    fn destination_never_overwrites_a_different_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("x.mkv");
        let pick = |total| match pick_destination(&dest, Some(total)).unwrap() {
            Picked::Finished(p) => format!("done {}", p.file_name().unwrap().to_string_lossy()),
            Picked::Write(c) => format!("write {}", c.0.file_name().unwrap().to_string_lossy()),
        };
        assert_eq!(pick(10), "write x.mkv");
        std::fs::write(&dest, [0u8; 10]).unwrap();
        assert_eq!(pick(10), "done x.mkv");
        assert_eq!(pick(20), "write x (2).mkv");
        std::fs::write(dir.path().join("x (2).mkv"), [0u8; 20]).unwrap();
        assert_eq!(pick(20), "done x (2).mkv");

        // Our partial download (same size) is resumed; someone else's
        // (another size) is left alone.
        Sidecar {
            total_size: 30,
            completed_ranges: vec![],
        }
        .save(&dir.path().join("y.mkv"))
        .unwrap();
        let ydest = dir.path().join("y.mkv");
        let picked = |total| match pick_destination(&ydest, Some(total)).unwrap() {
            Picked::Finished(p) => format!("done {}", p.file_name().unwrap().to_string_lossy()),
            Picked::Write(c) => format!("write {}", c.0.file_name().unwrap().to_string_lossy()),
        };
        assert_eq!(picked(30), "write y.mkv");
        assert_eq!(picked(40), "write y (2).mkv");
    }

    #[test]
    fn concurrent_downloads_never_share_a_path() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("z.bin");
        let first = match pick_destination(&dest, Some(5)).unwrap() {
            Picked::Write(c) => c,
            Picked::Finished(_) => panic!(),
        };
        let second = match pick_destination(&dest, Some(5)).unwrap() {
            Picked::Write(c) => c,
            Picked::Finished(_) => panic!(),
        };
        assert_ne!(first.0, second.0);
        drop(first);
        // Released paths can be picked again.
        match pick_destination(&dest, Some(5)).unwrap() {
            Picked::Write(c) => assert_eq!(c.0, dest),
            Picked::Finished(_) => panic!(),
        }
    }

    #[test]
    fn long_names_are_shortened_keeping_the_extension() {
        let long = format!("{}.mkv", "é".repeat(150)); // 300 bytes
        let short = safe_filename(&long);
        assert!(short.len() <= MAX_NAME_BYTES, "{}", short.len());
        assert!(short.ends_with(".mkv"));
        assert_eq!(safe_filename("short.mkv"), "short.mkv");
    }

    #[test]
    fn unique_names_and_connection_caps() {
        let mut claimed = HashSet::new();
        assert_eq!(unique_name(&mut claimed, "x.mkv"), "x.mkv");
        assert_eq!(unique_name(&mut claimed, "x.mkv"), "x (2).mkv");
        assert_eq!(unique_name(&mut claimed, "X.MKV"), "X (3).MKV");
        assert_eq!(unique_name(&mut claimed, "README"), "README");
        assert_eq!(unique_name(&mut claimed, "README"), "README (2)");
        assert_eq!(cap_connections(8, 4), 4);
        assert_eq!(cap_connections(8, 0), 8);
        assert_eq!(cap_connections(0, 16), 1);
    }

    #[test]
    fn safe_filename_strips_paths() {
        assert_eq!(safe_filename("movie.mkv"), "movie.mkv");
        assert_eq!(safe_filename("../../etc/passwd"), "passwd");
        assert_eq!(safe_filename("a\\b.txt"), "b.txt");
        assert_eq!(safe_filename(".."), "download");
        assert_eq!(safe_filename("dir/"), "download");
    }

    #[tokio::test]
    async fn segmented_download_resumes_with_a_new_url() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

        struct RangeResponder(Vec<u8>);
        impl Respond for RangeResponder {
            fn respond(&self, req: &Request) -> ResponseTemplate {
                let range = req.headers.get("range").unwrap().to_str().unwrap();
                let (s, e) = range.trim_start_matches("bytes=").split_once('-').unwrap();
                let (s, e): (usize, usize) = (s.parse().unwrap(), e.parse().unwrap());
                ResponseTemplate::new(206)
                    .insert_header("content-range", format!("bytes {s}-{e}/{}", self.0.len()))
                    .set_body_bytes(self.0[s..=e].to_vec())
            }
        }

        let total = (CHUNK_SIZE * 2 + 123) as usize;
        let data: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(RangeResponder(data.clone()))
            .mount(&server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path().to_path_buf();
        let dest = dir.join("out.bin");

        // Pretend a previous run (with a different URL) finished the first chunk.
        std::fs::write(&dest, &data[..CHUNK_SIZE as usize]).unwrap();
        Sidecar {
            total_size: total as u64,
            completed_ranges: vec![(0, CHUNK_SIZE - 1)],
        }
        .save(&dest)
        .unwrap();

        let opts = DownloadOpts {
            concurrency: 3,
            quiet: true,
        };
        fetch(&format!("{}/new", server.uri()), "out.bin", &dest, opts)
            .await
            .unwrap();

        let got = std::fs::read(&dest).unwrap();
        let sidecar_left = Sidecar::path_for(&dest).exists();
        let ranges_requested = server.received_requests().await.unwrap().len();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(got == data, "downloaded bytes differ");
        assert!(!sidecar_left);
        // 1 probe + 2 remaining chunks; the finished chunk was not refetched.
        assert_eq!(ranges_requested, 3);
    }
}
