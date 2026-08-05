use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
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

/// Number of attempts (including the first) for each segment before giving up.
const MAX_ATTEMPTS: u32 = 3;

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

    fn load(dest: &Path, url: &str, total_size: u64) -> Option<Sidecar> {
        let path = Self::path_for(dest);
        let data = std::fs::read(&path).ok()?;
        let sidecar: Sidecar = serde_json::from_slice(&data).ok()?;
        if sidecar.url == url && sidecar.total_size == total_size {
            Some(sidecar)
        } else {
            None
        }
    }

    fn save(&self, dest: &Path) -> Result<()> {
        let path = Self::path_for(dest);
        let data = serde_json::to_vec_pretty(self)?;
        std::fs::write(&path, data)?;
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

async fn probe(client: &Client, url: &str) -> Result<ProbeResult> {
    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()
        .await
        .context("failed to probe URL for range support")?;

    let status = resp.status();
    let headers = resp.headers().clone();

    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        // Content-Range: bytes 0-0/12345
        let total_size = headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next())
            .and_then(|v| v.trim().parse::<u64>().ok());
        return Ok(ProbeResult {
            supports_ranges: true,
            total_size,
        });
    }

    let accepts_ranges = headers
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("bytes"))
        .unwrap_or(false);

    let content_length = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    if accepts_ranges && content_length.is_some() {
        return Ok(ProbeResult {
            supports_ranges: true,
            total_size: content_length,
        });
    }

    // No range support (or we can't confirm it). We may still know the size
    // from a plain 200 response's Content-Length, which is useful for the
    // fallback progress bar.
    Ok(ProbeResult {
        supports_ranges: false,
        total_size: content_length,
    })
}

/// Downloads `url` to `dest`, streaming straight to disk (never buffering the
/// whole file in memory). Uses multi-segment parallel range requests for large,
/// range-capable downloads and falls back to a single stream otherwise.
/// Resumable: interrupted downloads leave a `<dest>.rdpart` sidecar that is
/// picked back up on the next call with the same `dest`.
pub async fn fetch(url: &str, dest: &Path, opts: DownloadOpts) -> Result<()> {
    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    let probe_result = probe(&client, url).await?;

    let use_segmented = probe_result.supports_ranges
        && probe_result
            .total_size
            .map(|sz| sz > SEGMENT_THRESHOLD)
            .unwrap_or(false)
        && opts.concurrency > 1;

    if use_segmented {
        let total_size = probe_result.total_size.expect("checked above");
        fetch_segmented(&client, url, dest, total_size, opts).await
    } else {
        fetch_single_stream(&client, url, dest, probe_result.total_size, opts).await
    }
}

fn make_progress_bar(total: Option<u64>, quiet: bool) -> ProgressBar {
    if quiet {
        return ProgressBar::hidden();
    }
    match total {
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
    }
}

async fn fetch_single_stream(
    client: &Client,
    url: &str,
    dest: &Path,
    total_size: Option<u64>,
    opts: DownloadOpts,
) -> Result<()> {
    let resp = client
        .get(url)
        .send()
        .await
        .context("request failed")?
        .error_for_status()
        .context("server returned an error status")?;

    let total_size = total_size.or_else(|| resp.content_length());

    let mut file = tokio::fs::File::create(dest)
        .await
        .with_context(|| format!("failed to create {}", dest.display()))?;

    let pb = make_progress_bar(total_size, opts.quiet);

    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error while streaming response body")?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
            .await
            .context("failed to write to destination file")?;
        pb.inc(chunk.len() as u64);
    }

    tokio::io::AsyncWriteExt::flush(&mut file).await?;
    pb.finish_and_clear();

    Ok(())
}

/// Splits `[0, total_size)` into `n` roughly-equal inclusive byte ranges.
fn split_ranges(total_size: u64, n: usize) -> Vec<(u64, u64)> {
    let n = n.max(1) as u64;
    let base = total_size / n;
    let mut ranges = Vec::new();
    let mut start = 0u64;
    for i in 0..n {
        if start >= total_size {
            break;
        }
        let end = if i == n - 1 {
            total_size - 1
        } else {
            (start + base - 1).min(total_size - 1)
        };
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
    let all_ranges = split_ranges(total_size, opts.concurrency);

    let existing_sidecar = Sidecar::load(dest, url, total_size);
    let completed: Vec<(u64, u64)> = existing_sidecar
        .as_ref()
        .map(|s| s.completed_ranges.clone())
        .unwrap_or_default();

    // Preallocate/open the file. If resuming, don't truncate what's there.
    let resuming = existing_sidecar.is_some() && dest.exists();
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

    let sidecar = Arc::new(tokio::sync::Mutex::new(Sidecar {
        url: url.to_string(),
        total_size,
        completed_ranges: completed.clone(),
    }));
    // Persist an initial sidecar right away so a crash right after start
    // still leaves something resumable/consistent.
    sidecar.lock().await.save(dest)?;

    let already_done_bytes: u64 = completed.iter().map(|(s, e)| e - s + 1).sum();
    let bytes_done = Arc::new(AtomicU64::new(already_done_bytes));

    let pb = make_progress_bar(Some(total_size), opts.quiet);
    pb.set_position(already_done_bytes);

    let pending_ranges: Vec<(u64, u64)> = all_ranges
        .into_iter()
        .filter(|r| !completed.contains(r))
        .collect();

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
                sc.completed_ranges.push(range);
                sc.save(&dest)?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    let mut first_error: Option<anyhow::Error> = None;
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(join_err) => {
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("segment task panicked: {join_err}"));
                }
            }
        }
    }

    pb.finish_and_clear();

    if let Some(e) = first_error {
        return Err(e);
    }

    Sidecar::delete(dest);

    Ok(())
}

async fn download_segment(
    client: &Client,
    url: &str,
    dest: &Path,
    range: (u64, u64),
    bytes_done: &Arc<AtomicU64>,
    pb: &ProgressBar,
) -> Result<()> {
    let (start, end) = range;

    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match download_segment_once(client, url, dest, range, bytes_done, pb).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt >= MAX_ATTEMPTS {
                    return Err(e.context(format!(
                        "failed to download byte range {start}-{end} after {attempt} attempts"
                    )));
                }
                let backoff = Duration::from_millis(200 * 2u64.pow(attempt - 1));
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

async fn download_segment_once(
    client: &Client,
    url: &str,
    dest: &Path,
    range: (u64, u64),
    bytes_done: &Arc<AtomicU64>,
    pb: &ProgressBar,
) -> Result<()> {
    let (start, end) = range;

    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(dest)
        .with_context(|| format!("failed to open {} for writing", dest.display()))?;
    let file = Arc::new(file);

    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
        .send()
        .await
        .with_context(|| format!("request failed for range {start}-{end}"))?
        .error_for_status()
        .with_context(|| format!("server returned an error status for range {start}-{end}"))?;

    let mut stream = resp.bytes_stream();
    let mut offset = start;
    // Track bytes written in this attempt so we can roll back the shared
    // counter/progress bar if this attempt fails partway and gets retried.
    let mut written_this_attempt: u64 = 0;

    let result: Result<()> = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.with_context(|| {
                format!("error while streaming response body for range {start}-{end}")
            })?;
            let chunk_len = chunk.len() as u64;
            let write_offset = offset;
            let file = Arc::clone(&file);
            tokio::task::spawn_blocking(move || -> Result<()> {
                write_all_at(&file, &chunk, write_offset).context("positioned write failed")?;
                Ok(())
            })
            .await
            .context("blocking write task panicked")??;

            offset += chunk_len;
            written_this_attempt += chunk_len;
            bytes_done.fetch_add(chunk_len, Ordering::Relaxed);
            pb.set_position(bytes_done.load(Ordering::Relaxed));
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        // Roll back progress attributed to this failed attempt so a retry
        // doesn't double count / overshoot the total.
        bytes_done.fetch_sub(written_this_attempt, Ordering::Relaxed);
        pb.set_position(bytes_done.load(Ordering::Relaxed));
        return Err(e);
    }

    let expected = end - start + 1;
    if written_this_attempt != expected {
        bytes_done.fetch_sub(written_this_attempt, Ordering::Relaxed);
        pb.set_position(bytes_done.load(Ordering::Relaxed));
        anyhow::bail!(
            "short read for range {start}-{end}: expected {expected} bytes, got {written_this_attempt}"
        );
    }

    Ok(())
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
