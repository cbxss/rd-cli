use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct DownloadOpts {
    /// Max concurrent range requests for the segmented path.
    pub concurrency: usize,
    /// Suppress the progress bar.
    pub quiet: bool,
}

impl Default for DownloadOpts {
    fn default() -> Self {
        Self {
            concurrency: 8,
            quiet: false,
        }
    }
}

/// Below this size we don't bother segmenting even if the server supports
/// ranges - the overhead of spawning multiple connections isn't worth it.
const SEGMENT_THRESHOLD: u64 = 50 * 1024 * 1024;

/// Size of each range request in the segmented path. Fixed-size chunks (rather
/// than one segment per connection) keep a failed attempt and an interrupted
/// run cheap: at most one chunk per connection is lost, which matters for
/// multi-hundred-GB files, and resume works regardless of `--concurrency`.
const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Number of attempts (including the first) for each segment before giving up.
const MAX_ATTEMPTS: u32 = 5;

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

/// An error that retrying won't fix (an expired link, a server that ignores
/// ranges), so the download stops instead of burning through retries.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Fatal(String);

fn is_fatal(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<Fatal>())
}

/// Like `error_for_status`, but 4xx responses (other than timeouts and rate
/// limits) are [`Fatal`].
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
        return Err(Fatal(msg).into());
    }
    Err(anyhow::anyhow!(msg))
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(200 * 2u64.pow(attempt.saturating_sub(1).min(6)))
}

/// Fails before any data is fetched when the destination's filesystem can't
/// hold the `needed` bytes still to download. If free space can't be
/// determined, the download goes ahead.
fn ensure_space(dest: &Path, needed: u64) -> Result<()> {
    let dir = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    match fs4::available_space(dir) {
        Ok(free) => check_space(needed, free, dir),
        Err(_) => Ok(()),
    }
}

fn check_space(needed: u64, free: u64, dir: &Path) -> Result<()> {
    if needed > free {
        anyhow::bail!(
            "not enough disk space in {}: need {}, only {} free",
            dir.display(),
            crate::output::human_bytes(needed as i64),
            crate::output::human_bytes(free as i64),
        );
    }
    Ok(())
}

/// Reduces a server-supplied file name to a single path component so it can't
/// escape the output directory (`../x`, `/etc/x`, `a\\b`).
pub fn safe_filename(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("").trim();
    if base.is_empty() || base == "." || base == ".." {
        "download".to_string()
    } else {
        base.to_string()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Sidecar {
    url: String,
    total_size: u64,
    completed_ranges: Vec<(u64, u64)>,
}

impl Sidecar {
    fn path_for(dest: &Path) -> PathBuf {
        PathBuf::from(format!("{}.rdpart", dest.display()))
    }

    /// Loads a sidecar for `dest` if it describes a file of the same size. The
    /// URL is deliberately not compared: Real-Debrid can hand out a different
    /// download URL each time a link is unrestricted, and discarding (and
    /// truncating) hundreds of GB of progress because of that is far worse.
    fn load(dest: &Path, total_size: u64) -> Option<Sidecar> {
        let path = Self::path_for(dest);
        let data = std::fs::read(&path).ok()?;
        let sidecar: Sidecar = serde_json::from_slice(&data).ok()?;
        if sidecar.total_size == total_size {
            Some(sidecar)
        } else {
            None
        }
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

    fn save(&self, dest: &Path) -> Result<()> {
        let path = Self::path_for(dest);
        let data = serde_json::to_vec(self)?;
        // Write-then-rename so an interrupt can't leave a half-written sidecar
        // (which would be ignored, and the partial download truncated).
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn delete(dest: &Path) {
        let path = Self::path_for(dest);
        let _ = std::fs::remove_file(path);
    }
}

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

/// Downloads `url` to `dest`, streaming straight to disk (never buffering the
/// whole file in memory). Uses multi-segment parallel range requests for large,
/// range-capable downloads and falls back to a single stream otherwise.
/// Resumable: interrupted downloads leave a `<dest>.rdpart` sidecar that is
/// picked back up on the next call with the same `dest`.
pub async fn fetch(url: &str, dest: &Path, opts: DownloadOpts) -> Result<()> {
    let client = http()?;
    let probe_result = probe(client, url).await?;

    // A file of the expected size with no `.rdpart` sidecar is a finished
    // earlier download; don't fetch it again.
    if let Some(total) = probe_result.total_size {
        let finished = !Sidecar::path_for(dest).exists()
            && std::fs::metadata(dest).is_ok_and(|m| m.is_file() && m.len() == total);
        if finished {
            if !opts.quiet {
                status(&format!("already downloaded: {}", dest.display()));
            }
            return Ok(());
        }
    }

    let use_segmented = probe_result.supports_ranges
        && probe_result
            .total_size
            .map(|sz| sz > SEGMENT_THRESHOLD)
            .unwrap_or(false)
        && opts.concurrency > 1;

    if use_segmented {
        let total_size = probe_result.total_size.expect("checked above");
        fetch_segmented(client, url, dest, total_size, opts).await
    } else {
        fetch_single_stream(client, url, dest, probe_result.total_size, opts).await
    }
}

fn make_progress_bar(total: Option<u64>, quiet: bool) -> ProgressBar {
    if quiet {
        return ProgressBar::hidden();
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
    bars().add(pb)
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
    let pb = make_progress_bar(total_size, opts.quiet);

    let mut attempt = 0u32;
    let result = loop {
        attempt += 1;
        pb.set_position(0);
        match single_stream_once(client, url, &tmp, &pb).await {
            Ok(()) => break Ok(()),
            Err(e) if is_fatal(&e) || attempt >= MAX_ATTEMPTS => break Err(e),
            Err(_) => tokio::time::sleep(retry_delay(attempt)).await,
        }
    };
    pb.finish_and_clear();

    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, dest)
        .with_context(|| format!("failed to move download into place at {}", dest.display()))
}

async fn single_stream_once(
    client: &Client,
    url: &str,
    tmp: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    let resp = client.get(url).send().await.context("request failed")?;
    let resp = check_status(resp, "download failed")?;
    let expected = resp.content_length();
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

    // Preallocate/open the file. If resuming, don't truncate what's there.
    let existing_sidecar = Sidecar::load(dest, total_size).filter(|_| dest.exists());
    let resuming = existing_sidecar.is_some();
    let sidecar = Sidecar {
        url: url.to_string(),
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
    // sparse, so a full disk would otherwise only show up hours in.
    ensure_space(dest, total_size - already_done_bytes)?;

    // Save the sidecar before creating the file: a full-size file without
    // one would look finished to the "already downloaded" check.
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

    let pb = make_progress_bar(Some(total_size), opts.quiet);
    pb.set_position(already_done_bytes);

    let mut tasks = tokio::task::JoinSet::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(opts.concurrency.max(1)));

    for range in pending_ranges {
        let client = client.clone();
        let url = url.to_string();
        let dest = dest.to_path_buf();
        let bytes_done = Arc::clone(&bytes_done);
        let pb = pb.clone();
        let sem = Arc::clone(&sem);
        let sidecar = Arc::clone(&sidecar);

        tasks.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            download_segment(&client, &url, &dest, range, &bytes_done, &pb).await?;
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
        return Err(e);
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
        let from = start + written;
        let (progress, result) =
            download_range_once(client, url, &file, (from, end), bytes_done, pb).await;
        written += progress;
        match result {
            Ok(()) => return Ok(()),
            Err(e) if is_fatal(&e) => return Err(e),
            Err(e) => {
                stalled_attempts = if progress > 0 {
                    1
                } else {
                    stalled_attempts + 1
                };
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
            return Err(Fatal(format!(
                "server ignored the range request for {start}-{end} (HTTP {}); retry with --concurrency 1",
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
            return Err(Fatal(format!(
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
            url: String::new(),
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

    fn temp_dest(tag: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("rd-dl-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("out.bin");
        (dir, dest)
    }

    #[tokio::test]
    async fn segmented_download_rejects_ignored_ranges() {
        let total = (SEGMENT_THRESHOLD + 10) as usize;
        let server = range_ignoring_server(vec![7u8; total]).await;
        let (dir, dest) = temp_dest("norange");
        let opts = DownloadOpts {
            concurrency: 2,
            quiet: true,
        };
        let err = fetch(&server.uri(), &dest, opts).await.unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(
            format!("{err:#}").contains("ignored the range request"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn finished_file_is_not_downloaded_again() {
        let data = vec![1u8; 1000];
        let server = range_ignoring_server(data.clone()).await;
        let (dir, dest) = temp_dest("done");
        std::fs::write(&dest, &data).unwrap();
        fetch(
            &server.uri(),
            &dest,
            DownloadOpts {
                concurrency: 1,
                quiet: true,
            },
        )
        .await
        .unwrap();
        let requests = server.received_requests().await.unwrap().len();
        std::fs::remove_dir_all(&dir).unwrap();
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

        let dir = std::env::temp_dir().join(format!("rd-dl-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("out.bin");

        // Pretend a previous run (with a different URL) finished the first chunk.
        std::fs::write(&dest, &data[..CHUNK_SIZE as usize]).unwrap();
        Sidecar {
            url: "https://old.example/expired".into(),
            total_size: total as u64,
            completed_ranges: vec![(0, CHUNK_SIZE - 1)],
        }
        .save(&dest)
        .unwrap();

        let opts = DownloadOpts {
            concurrency: 3,
            quiet: true,
        };
        fetch(&format!("{}/new", server.uri()), &dest, opts)
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
