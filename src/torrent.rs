//! The torrent workflow shared by `rd dl` and `rd torrents`: add (or reuse)
//! a torrent, select files once Real-Debrid is ready, wait for it to finish,
//! then unrestrict and download its links.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{StreamExt, stream};

use crate::api::Client;
use crate::api::error::RdError;
use crate::api::models::{AddMagnetResponse, Torrent, TorrentInfo, UnrestrictedLink};
use crate::download::{self, DownloadOpts, Report};

pub const TERMINAL_FAILURE_STATUSES: &[&str] = &["error", "virus", "dead", "magnet_error"];

/// Stall timeout for commands that don't expose `--stall-timeout`.
pub const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Number of consecutive polls a torrent may report `downloaded` without
/// links before we give up (in case the links lag the status briefly).
const EMPTY_LINKS_TOLERANCE: u32 = 3;

/// How often torrent status is polled. `RD_POLL_MS` overrides it (used by the
/// end-to-end tests to keep slow-torrent scenarios fast).
fn poll_interval() -> Duration {
    if cfg!(test) {
        return Duration::from_millis(5);
    }
    std::env::var("RD_POLL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(5))
}

/// Real-Debrid answers an unknown torrent ID with `unknown_ressource` (7), or
/// `unknown_method` (3) when the ID doesn't match its routing pattern.
pub fn is_not_found(err: &RdError) -> bool {
    matches!(
        err,
        RdError::Api {
            code: 3 | 7 | 404,
            ..
        }
    )
}

pub async fn get_info(client: &Client, id: &str) -> Result<TorrentInfo> {
    match client.get_json(&format!("/torrents/info/{id}")).await {
        Ok(info) => Ok(info),
        Err(err) if is_not_found(&err) => anyhow::bail!(
            "torrent {id} not found on this account — it may have been deleted (see `rd torrents list`)"
        ),
        Err(err) => Err(err.into()),
    }
}

/// `get_info` for the polling loops: a temporary failure (network trouble,
/// a 5xx, rate limiting) is reported as `Ok(None)` so a long wait survives a
/// short Real-Debrid outage; the stall timeout decides when to give up.
async fn poll_info(client: &Client, id: &str) -> Result<Option<TorrentInfo>> {
    match client.get_json(&format!("/torrents/info/{id}")).await {
        Ok(info) => Ok(Some(info)),
        Err(err) if is_not_found(&err) => anyhow::bail!(
            "torrent {id} not found on this account — it may have been deleted (see `rd torrents list`)"
        ),
        Err(RdError::Http(_) | RdError::RateLimited) => Ok(None),
        Err(RdError::Api { status, .. }) if status >= 500 => Ok(None),
        Err(err) => Err(err.into()),
    }
}

// ---- adding ----

/// Extracts the lowercase hex info-hash from a magnet's `xt=urn:btih:`
/// parameter, accepting the 40-char hex and 32-char base32 forms.
pub fn magnet_hash(magnet: &str) -> Option<String> {
    let query = magnet.strip_prefix("magnet:?")?;
    query
        .split('&')
        .map(|kv| kv.strip_prefix("amp;").unwrap_or(kv))
        .find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            // `xt`, or numbered `xt.1`, `xt.2`, ... in any case.
            let k = k.to_ascii_lowercase();
            if k != "xt"
                && !k
                    .strip_prefix("xt.")
                    .is_some_and(|n| n.parse::<u32>().is_ok())
            {
                return None;
            }
            let v = v.to_ascii_lowercase().replace("%3a", ":");
            let hash = v.strip_prefix("urn:btih:")?;
            if hash.len() == 40 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
                Some(hash.to_string())
            } else if hash.len() == 32 {
                base32_to_hex(hash)
            } else {
                None
            }
        })
}

/// RFC 4648 base32 (no padding) to lowercase hex.
fn base32_to_hex(s: &str) -> Option<String> {
    let mut bits = 0u64;
    let mut nbits = 0u32;
    let mut out = String::with_capacity(40);
    for c in s.chars() {
        let v = match c.to_ascii_lowercase() {
            c @ 'a'..='z' => c as u64 - 'a' as u64,
            c @ '2'..='7' => c as u64 - '2' as u64 + 26,
            _ => return None,
        };
        bits = (bits << 5) | v;
        nbits += 5;
        if nbits >= 8 {
            nbits -= 8;
            out.push_str(&format!("{:02x}", (bits >> nbits) & 0xff));
        }
    }
    (out.len() == 40).then_some(out)
}

/// Whether Real-Debrid is still before the file-selection step, so a
/// selection can still be made.
pub fn awaiting_selection(info: &TorrentInfo) -> bool {
    matches!(
        info.status.as_str(),
        "magnet_conversion" | "waiting_files_selection"
    )
}

/// Whether the torrent's selected files are exactly `files` ("all" or
/// comma-separated ids).
pub fn selection_matches(info: &TorrentInfo, files: &str) -> bool {
    if files == "all" {
        return info.files.iter().all(|f| f.selected != 0);
    }
    let want: HashSet<i64> = files.split(',').filter_map(|x| x.parse().ok()).collect();
    let have: HashSet<i64> = info
        .files
        .iter()
        .filter(|f| f.selected != 0)
        .map(|f| f.id)
        .collect();
    want == have
}

/// Fails if `id` already has a selection other than `files`: Real-Debrid
/// can't change a selection once made, and downloading the old one would
/// silently ignore `--select`.
pub async fn ensure_selection(client: &Client, id: &str, files: &str) -> Result<()> {
    let info = get_info(client, id).await?;
    if awaiting_selection(&info) || selection_matches(&info, files) {
        return Ok(());
    }
    let have: Vec<String> = info
        .files
        .iter()
        .filter(|f| f.selected != 0)
        .map(|f| f.id.to_string())
        .collect();
    anyhow::bail!(
        "torrent {id} already has files {} selected, not {files}, and Real-Debrid can't change a \
         selection once made. Download it as it is with `rd dl {id}`, or pass the magnet to \
         `rd dl --select {files}`, which adds a new copy.",
        have.join(",")
    )
}

/// Finds a torrent already on the account with this hash that hasn't failed
/// and will deliver exactly `files` (not yet selected, or that selection),
/// preferring one that has finished downloading. A copy with a different
/// selection is skipped.
async fn find_existing(
    client: &Client,
    torrents: &[Torrent],
    hash: &str,
    files: &str,
) -> Result<Option<Torrent>> {
    let mut candidates: Vec<Torrent> = torrents
        .iter()
        .filter(|t| {
            t.hash.eq_ignore_ascii_case(hash)
                && !TERMINAL_FAILURE_STATUSES.contains(&t.status.as_str())
        })
        .cloned()
        .collect();
    candidates.sort_by_key(|t| t.status != "downloaded");
    for t in candidates {
        // Deleted since the listing: just not a candidate any more.
        let Ok(info) = get_info(client, &t.id).await else {
            continue;
        };
        if awaiting_selection(&info) || selection_matches(&info, files) {
            return Ok(Some(t));
        }
    }
    Ok(None)
}

/// Attempts at `addMagnet` when the outcome of a request is unknown.
const ADD_ATTEMPTS: u32 = 3;

/// A failure after which we can't tell whether the server acted on the
/// request: a 5xx, or a connection lost after the request was sent.
fn outcome_unknown(err: &RdError) -> bool {
    match err {
        RdError::Http(e) => !e.is_connect(),
        RdError::Api { status, .. } => *status >= 500,
        _ => false,
    }
}

fn ids_with_hash(torrents: &[Torrent], hash: &str) -> Vec<String> {
    torrents
        .iter()
        .filter(|t| t.hash.eq_ignore_ascii_case(hash))
        .map(|t| t.id.clone())
        .collect()
}

async fn list_torrents(client: &Client) -> Result<Vec<Torrent>> {
    Ok(client.get_all_pages("/torrents", &[]).await?)
}

/// Adds a magnet. `addMagnet` isn't idempotent, so it's never blindly
/// retried: after an ambiguous failure we look for a torrent with the
/// magnet's hash that wasn't there before (`before`: IDs with that hash from
/// a listing taken before the first attempt), and only re-send if there's
/// none. Magnets whose hash we can't read are sent once.
async fn add_magnet(
    client: &Client,
    link: &str,
    host: Option<&str>,
    hash: Option<String>,
    before: Option<Vec<String>>,
) -> Result<String> {
    let mut form: Vec<(&str, &str)> = vec![("magnet", link)];
    if let Some(h) = host {
        form.push(("host", h));
    }
    let before = match (&hash, before) {
        (Some(_), Some(ids)) => ids,
        (Some(h), None) => ids_with_hash(&list_torrents(client).await?, h),
        (None, _) => vec![],
    };

    let mut attempt = 0;
    loop {
        attempt += 1;
        let err = match client
            .post_form_once::<AddMagnetResponse>("/torrents/addMagnet", &form)
            .await
        {
            Ok(resp) => return Ok(resp.id),
            Err(err) => err,
        };
        if !outcome_unknown(&err) {
            return Err(err).context("failed to add magnet");
        }
        let Some(h) = &hash else {
            return Err(err).context(
                "failed to add magnet; it may have been added anyway — check `rd torrents list` before retrying",
            );
        };
        // Real-Debrid may have created it before failing; never send twice
        // without looking (including after the last attempt).
        tokio::time::sleep(poll_interval()).await;
        let created = ids_with_hash(&list_torrents(client).await?, h)
            .into_iter()
            .find(|id| !before.contains(id));
        if let Some(id) = created {
            return Ok(id);
        }
        if attempt >= ADD_ATTEMPTS {
            return Err(err).context("failed to add magnet");
        }
    }
}

/// Adds a magnet link or `.torrent` file; returns the new torrent's ID.
pub async fn add(client: &Client, link: &str, host: Option<&str>) -> Result<String> {
    if link.starts_with("magnet:") {
        return add_magnet(client, link, host, magnet_hash(link), None).await;
    }
    let resp: AddMagnetResponse = {
        let bytes = tokio::fs::read(link)
            .await
            .with_context(|| format!("failed to read torrent file {link}"))?;
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(h) = host {
            query.push(("host", h));
        }
        client
            .put_bytes("/torrents/addTorrent", &query, bytes)
            .await
            .context("failed to add torrent file")?
    };
    Ok(resp.id)
}

/// Like [`add`], but reuses a torrent with the same magnet hash that will
/// deliver `files`.
pub async fn add_or_reuse(
    client: &Client,
    link: &str,
    host: Option<&str>,
    files: &str,
    quiet: bool,
) -> Result<String> {
    let id = match magnet_hash(link) {
        Some(hash) => {
            let torrents = list_torrents(client).await?;
            if let Some(existing) = find_existing(client, &torrents, &hash, files).await? {
                if !quiet {
                    download::status(&format!(
                        "reusing existing torrent {} ({})",
                        existing.id, existing.status
                    ));
                }
                return Ok(existing.id);
            }
            let before = ids_with_hash(&torrents, &hash);
            add_magnet(client, link, host, Some(hash), Some(before)).await?
        }
        None => add(client, link, host).await?,
    };
    if !quiet {
        download::status(&format!("added torrent {id}"));
    }
    Ok(id)
}

// ---- waiting ----

/// Tracks whether a torrent is still moving. Any change in status or progress
/// resets the clock.
struct StallGuard {
    timeout: Option<Duration>,
    last: Option<(String, u64)>,
    since: Instant,
}

impl StallGuard {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            last: None,
            since: Instant::now(),
        }
    }

    fn check(&mut self, id: &str, info: &TorrentInfo) -> Result<()> {
        // Real-Debrid reports whole percentages, so a big, slow torrent can
        // sit on one value for a long time while bytes are still arriving:
        // a non-zero speed counts as progress. Queued/compressing/uploading
        // are Real-Debrid's own stages, not a stalled swarm.
        let moving = info.speed.is_some_and(|s| s > 0)
            || matches!(info.status.as_str(), "queued" | "compressing" | "uploading");
        let state = (info.status.clone(), info.progress.to_bits());
        if moving || self.last.as_ref() != Some(&state) {
            self.last = Some(state);
            self.since = Instant::now();
            return Ok(());
        }
        let Some(timeout) = self.timeout else {
            return Ok(());
        };
        if self.since.elapsed() >= timeout {
            let seeders = info
                .seeders
                .map(|s| format!(", {s} seeders"))
                .unwrap_or_default();
            return Err(download::Incomplete(format!(
                "torrent {id} made no progress for {} (status '{}', {:.1}%{seeders}). \
                 It is still on Real-Debrid: run `rd dl {id}` to keep waiting, or \
                 `rd torrents delete {id}` to give up.",
                human_duration(timeout),
                info.status,
                info.progress,
            ))
            .into());
        }
        Ok(())
    }
}

impl StallGuard {
    /// Called when a poll failed temporarily: no news counts as no progress.
    fn check_unreachable(&self, id: &str) -> Result<()> {
        match self.timeout {
            Some(timeout) if self.since.elapsed() >= timeout => Err(download::Incomplete(format!(
                "couldn't reach Real-Debrid for {} while waiting on torrent {id}; run `rd dl {id}` to keep waiting",
                human_duration(timeout)
            ))
            .into()),
            _ => Ok(()),
        }
    }
}

fn human_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 60 && secs % 60 == 0 {
        format!("{} min", secs / 60)
    } else {
        format!("{d:?}")
    }
}

async fn select_files(client: &Client, id: &str, files: &str) -> Result<()> {
    client
        .post_form_no_content(&format!("/torrents/selectFiles/{id}"), &[("files", files)])
        .await
        .context("failed to select files")
}

/// Waits out `magnet_conversion` (file IDs don't exist yet), then selects
/// `files` if Real-Debrid is waiting for a selection. Torrents already past
/// that point are left alone. Returns the torrent's state afterwards and
/// whether a selection was made.
pub async fn select_when_ready(
    client: &Client,
    id: &str,
    files: &str,
    quiet: bool,
    stall: Option<Duration>,
) -> Result<(TorrentInfo, bool)> {
    let mut announced = false;
    let mut guard = StallGuard::new(stall);
    loop {
        let Some(info) = poll_info(client, id).await? else {
            guard.check_unreachable(id)?;
            tokio::time::sleep(poll_interval()).await;
            continue;
        };
        match info.status.as_str() {
            "magnet_conversion" => {
                guard.check(id, &info)?;
                if !quiet && !announced {
                    download::status("waiting for Real-Debrid to convert the magnet...");
                    announced = true;
                }
                tokio::time::sleep(poll_interval()).await;
            }
            "waiting_files_selection" => {
                select_files(client, id, files).await?;
                return Ok((get_info(client, id).await?, true));
            }
            _ => return Ok((info, false)),
        }
    }
}

/// Drives a torrent to the `downloaded` state: selects files once Real-Debrid
/// is waiting for a selection (only then — a torrent already past that point
/// keeps its selection), then polls until links appear or a terminal failure
/// state is reached. Cached torrents resolve on the first check.
pub async fn wait_for_links(
    client: &Client,
    id: &str,
    files: &str,
    quiet: bool,
    stall: Option<Duration>,
) -> Result<TorrentInfo> {
    let mut guard = StallGuard::new(stall);
    let mut selected = false;
    let mut empty_links = 0u32;
    let mut last_line = String::new();
    loop {
        let Some(info) = poll_info(client, id).await? else {
            guard.check_unreachable(id)?;
            tokio::time::sleep(poll_interval()).await;
            continue;
        };

        match info.status.as_str() {
            "downloaded" if !info.links.is_empty() => return Ok(info),
            "downloaded" => {
                empty_links += 1;
                if empty_links >= EMPTY_LINKS_TOLERANCE {
                    anyhow::bail!(
                        "torrent {id} is downloaded but Real-Debrid returned no links (see `rd torrents info {id}`)"
                    );
                }
            }
            "waiting_files_selection" if !selected => {
                select_files(client, id, files).await?;
                selected = true;
                continue;
            }
            s if TERMINAL_FAILURE_STATUSES.contains(&s) => anyhow::bail!(
                "torrent {id} failed on Real-Debrid with status '{s}'. \
                 Delete it with `rd torrents delete {id}` before adding it again."
            ),
            _ => guard.check(id, &info)?,
        }

        if !quiet {
            let mut line = format!(
                "waiting on torrent: {} ({:.0}%)",
                info.status,
                info.progress.floor()
            );
            if let Some(seeders) = info.seeders {
                line.push_str(&format!(", {seeders} seeders"));
            }
            if line != last_line {
                download::status(&line);
                last_line = line;
            }
        }

        tokio::time::sleep(poll_interval()).await;
    }
}

// ---- downloading ----

#[derive(Debug, Clone, Copy)]
pub struct LinkOpts {
    pub quiet: bool,
    /// Connections per file.
    pub concurrency: usize,
    /// Files downloaded at the same time.
    pub jobs: usize,
}

/// Waits for the torrent, then downloads all of its links. With
/// `output_file`, the torrent must deliver exactly one file, saved under
/// that name.
pub async fn download(
    client: &Client,
    id: &str,
    files: &str,
    out_dir: &Path,
    opts: LinkOpts,
    stall: Option<Duration>,
    output_file: Option<&str>,
) -> Result<Report> {
    let info = wait_for_links(client, id, files, opts.quiet, stall).await?;
    if output_file.is_some() && info.links.len() != 1 {
        anyhow::bail!(
            "-O/--output-file needs a single file, but torrent {id} has {} links; \
             pick one with --select (file ids: `rd torrents info {id}`)",
            info.links.len()
        );
    }
    Ok(download_links(client, &info.links, out_dir, opts, output_file).await)
}

/// Waits for the torrent, then unrestricts its links and returns the direct
/// download URLs, without downloading anything.
pub async fn direct_links(
    client: &Client,
    id: &str,
    files: &str,
    quiet: bool,
    stall: Option<Duration>,
) -> Result<Vec<String>> {
    let info = wait_for_links(client, id, files, quiet, stall).await?;
    let mut urls = Vec::with_capacity(info.links.len());
    for link in &info.links {
        let ul: UnrestrictedLink = client
            .post_form("/unrestrict/link", &[("link", link.as_str())])
            .await
            .with_context(|| format!("failed to unrestrict {link}"))?;
        urls.push(ul.download);
    }
    Ok(urls)
}

/// Unrestricts and downloads `links` into `out_dir`, `opts.jobs` at a time.
/// Every link is attempted; files that fail get one more try on their own
/// (with a freshly unrestricted link), and remaining failures are reported
/// on stderr and in the returned [`Report`].
pub async fn download_links(
    client: &Client,
    links: &[String],
    out_dir: &Path,
    opts: LinkOpts,
    output_file: Option<&str>,
) -> Report {
    let mut names = Names {
        fixed: output_file.map(str::to_string),
        ..Names::default()
    };
    let mut report = Report::default();
    let (done, mut failed) = run_pass(
        client,
        links.iter().collect(),
        out_dir,
        opts,
        &mut names,
        opts.jobs,
    )
    .await;
    let mut done_paths = done;

    // A second, sequential pass: parallel downloads can hit limits a single
    // one doesn't (Real-Debrid's active-download cap, per-host connection
    // limits), and re-unrestricting replaces a link that expired.
    if !failed.is_empty() {
        if !opts.quiet {
            download::status(&format!(
                "retrying {} failed file(s) one at a time",
                failed.len()
            ));
        }
        let retry: Vec<&String> = failed.iter().map(|(link, _)| *link).collect();
        let (done, still_failed) = run_pass(client, retry, out_dir, opts, &mut names, 1).await;
        done_paths.extend(done);
        failed = still_failed;
    }

    for path in &done_paths {
        report.add_file(path);
    }
    for (link, err) in &failed {
        eprintln!("error: {err:#}");
        report.add_failure(link, err);
    }
    report
}

/// One pass over `batch`: unrestrict every link, name the files in link
/// order (so duplicate names resolve the same way on every run, however the
/// parallel requests finish), then download `jobs` at a time. Returns the
/// paths written and the links that failed.
async fn run_pass<'a>(
    client: &Client,
    batch: Vec<&'a String>,
    out_dir: &Path,
    opts: LinkOpts,
    names: &mut Names,
    jobs: usize,
) -> (Vec<PathBuf>, Vec<(&'a String, anyhow::Error)>) {
    let jobs = jobs.max(1);
    let resolved: Vec<(&String, Result<UnrestrictedLink>)> = stream::iter(batch)
        .map(|link| async move {
            let res = client
                .post_form("/unrestrict/link", &[("link", link.as_str())])
                .await
                .with_context(|| format!("failed to unrestrict {link}"));
            (link, res)
        })
        .buffered(jobs)
        .collect()
        .await;

    let mut failures = Vec::new();
    let mut work = Vec::new();
    for (link, res) in resolved {
        match res {
            Ok(ul) => {
                let name = names.for_link(link, &download::safe_filename(&ul.filename));
                work.push((link, ul, out_dir.join(name)));
            }
            Err(err) => failures.push((link, err)),
        }
    }

    let results: Vec<(&String, Result<PathBuf>)> = stream::iter(work)
        .map(|(link, ul, dest)| async move {
            let opts = DownloadOpts {
                concurrency: opts.concurrency,
                quiet: opts.quiet,
            };
            let res =
                download::fetch_file(&ul.download, &ul.filename, ul.chunks, &dest, opts).await;
            (link, res)
        })
        .buffer_unordered(jobs)
        .collect()
        .await;
    let mut done = Vec::new();
    for (link, res) in results {
        match res {
            Ok(path) => done.push(path),
            Err(e) => failures.push((link, e)),
        }
    }
    (done, failures)
}

/// Output names for the links of one download. Files in different torrent
/// folders can share a name ("Season 1/x.mkv", "Season 2/x.mkv") and
/// Real-Debrid only gives the base name, so each link claims a unique name —
/// and keeps it if it's retried, so a retry resumes the same file.
#[derive(Default)]
struct Names {
    taken: HashSet<String>,
    by_link: HashMap<String, String>,
    /// `-O`: the user's name for the (single) file.
    fixed: Option<String>,
}

impl Names {
    fn for_link(&mut self, link: &str, name: &str) -> String {
        if let Some(fixed) = &self.fixed {
            return fixed.clone();
        }
        if let Some(existing) = self.by_link.get(link) {
            return existing.clone();
        }
        let unique = download::unique_name(&mut self.taken, name);
        self.by_link.insert(link.to_string(), unique.clone());
        unique
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn info(status: &str, links: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "id": "ABC", "filename": "f", "hash": "h", "bytes": 1, "host": "rd",
            "split": 2000, "progress": 0.0, "status": status, "added": "x",
            "links": links,
        })
    }

    async fn mount_info_sequence(server: &MockServer, seq: &[serde_json::Value]) {
        for (i, body) in seq.iter().enumerate() {
            let m = Mock::given(method("GET"))
                .and(path("/torrents/info/ABC"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()));
            if i + 1 < seq.len() {
                m.up_to_n_times(1).mount(server).await;
            } else {
                m.mount(server).await;
            }
        }
    }

    #[test]
    fn magnet_hash_parsing() {
        let h = "0123456789ABCDEF0123456789abcdef01234567";
        assert_eq!(
            magnet_hash(&format!("magnet:?dn=x&xt=urn:btih:{h}&tr=y")).as_deref(),
            Some(h.to_ascii_lowercase().as_str())
        );
        assert_eq!(magnet_hash("magnet:?xt=urn:btih:SHORTBASE32"), None);
        assert_eq!(magnet_hash("https://example.com"), None);
        let lower = h.to_ascii_lowercase();
        assert_eq!(
            magnet_hash(&format!("magnet:?XT=urn:btih:{h}")).as_deref(),
            Some(lower.as_str())
        );
        assert_eq!(
            magnet_hash(&format!("magnet:?dn=a&amp;xt.1=urn:btih:{h}")).as_deref(),
            Some(lower.as_str())
        );
        // Base32 form, and a percent-encoded `urn:btih:`.
        assert_eq!(
            magnet_hash("magnet:?xt=urn:btih:AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH").as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(
            magnet_hash(&format!("magnet:?xt=urn%3Abtih%3A{h}")).as_deref(),
            Some(h.to_ascii_lowercase().as_str())
        );
    }

    fn listed(id: &str, hash: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id, "filename": "f", "hash": hash, "bytes": 1, "host": "rd",
            "split": 2000, "progress": 0.0, "status": "magnet_conversion", "added": "x",
        })
    }

    #[tokio::test]
    async fn add_magnet_does_not_duplicate_after_a_5xx() {
        let h = "0123456789abcdef0123456789abcdef01234567";
        let server = MockServer::start().await;
        // Before the add: an older copy. After the failed add: a new one too.
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([listed("OLD", h)])),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([listed("NEW", h), listed("OLD", h),])),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let id = add(&client, &format!("magnet:?xt=urn:btih:{h}"), None)
            .await
            .unwrap();
        assert_eq!(id, "NEW");
    }

    #[tokio::test]
    async fn add_magnet_resends_when_nothing_was_created() {
        let h = "0123456789abcdef0123456789abcdef01234567";
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_body_json(serde_json::json!({"id": "OK", "uri": "u"})),
            )
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let id = add(&client, &format!("magnet:?xt=urn:btih:{h}"), None)
            .await
            .unwrap();
        assert_eq!(id, "OK");
    }

    #[tokio::test]
    async fn already_downloaded_torrent_is_not_reselected() {
        let server = MockServer::start().await;
        mount_info_sequence(
            &server,
            &[info("downloaded", &["https://real-debrid.com/d/X"])],
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/torrents/selectFiles/ABC"))
            .respond_with(
                ResponseTemplate::new(404).set_body_json(
                    serde_json::json!({"error": "unknown_ressource", "error_code": 7}),
                ),
            )
            .expect(0)
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let got = wait_for_links(&client, "ABC", "all", true, None)
            .await
            .unwrap();
        assert_eq!(got.links.len(), 1);
    }

    #[tokio::test]
    async fn selects_only_once_conversion_finishes() {
        let server = MockServer::start().await;
        mount_info_sequence(
            &server,
            &[
                info("magnet_conversion", &[]),
                info("waiting_files_selection", &[]),
                info("downloading", &[]),
                info("downloaded", &["https://real-debrid.com/d/X"]),
            ],
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/torrents/selectFiles/ABC"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let got = wait_for_links(&client, "ABC", "all", true, None)
            .await
            .unwrap();
        assert_eq!(got.status, "downloaded");
    }

    #[tokio::test]
    async fn downloaded_without_links_fails_instead_of_looping() {
        let server = MockServer::start().await;
        mount_info_sequence(&server, &[info("downloaded", &[])]).await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = wait_for_links(&client, "ABC", "all", true, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no links"), "{err}");
    }

    #[tokio::test]
    async fn error_status_and_missing_torrent_are_reported() {
        let server = MockServer::start().await;
        mount_info_sequence(&server, &[info("error", &[])]).await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = wait_for_links(&client, "ABC", "all", true, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rd torrents delete ABC"), "{err}");

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/torrents/info/ABC"))
            .respond_with(
                ResponseTemplate::new(404).set_body_json(
                    serde_json::json!({"error": "unknown_ressource", "error_code": 7}),
                ),
            )
            .mount(&server)
            .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = wait_for_links(&client, "ABC", "all", true, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[tokio::test]
    async fn select_when_ready_skips_finished_torrents() {
        let server = MockServer::start().await;
        mount_info_sequence(
            &server,
            &[info("downloaded", &["https://real-debrid.com/d/X"])],
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/torrents/selectFiles/ABC"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let (got, selected) = select_when_ready(&client, "ABC", "all", true, None)
            .await
            .unwrap();
        assert!(!selected);
        assert_eq!(got.links.len(), 1);
    }

    #[tokio::test]
    async fn select_when_ready_waits_out_magnet_conversion() {
        let server = MockServer::start().await;
        mount_info_sequence(
            &server,
            &[
                info("magnet_conversion", &[]),
                info("magnet_conversion", &[]),
                info("waiting_files_selection", &[]),
                info("queued", &[]),
            ],
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/torrents/selectFiles/ABC"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let (got, selected) = select_when_ready(&client, "ABC", "all", true, None)
            .await
            .unwrap();
        assert!(selected);
        assert_eq!(got.status, "queued");
    }

    fn info_at(status: &str, progress: f64) -> serde_json::Value {
        let mut v = info(status, &[]);
        v["progress"] = serde_json::json!(progress);
        v
    }

    #[tokio::test]
    async fn stalled_torrent_times_out() {
        let server = MockServer::start().await;
        mount_info_sequence(&server, &[info_at("downloading", 0.0)]).await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = wait_for_links(&client, "ABC", "all", true, Some(Duration::from_millis(30)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("made no progress"), "{err}");
    }

    #[tokio::test]
    async fn stall_clock_resets_on_progress() {
        // Each state is seen on two consecutive polls (~10 ms apart), the
        // timeout is 200 ms, and the whole run takes well over 200 ms: it can
        // only succeed if every change restarts the clock.
        let server = MockServer::start().await;
        let mut seq = Vec::new();
        for p in 1..=60 {
            seq.push(info_at("downloading", p as f64));
            seq.push(info_at("downloading", p as f64));
        }
        seq.push(info("downloaded", &["https://real-debrid.com/d/X"]));
        mount_info_sequence(&server, &seq).await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let started = Instant::now();
        let got = wait_for_links(
            &client,
            "ABC",
            "all",
            true,
            Some(Duration::from_millis(200)),
        )
        .await
        .unwrap();
        assert_eq!(got.status, "downloaded");
        assert!(
            started.elapsed() > Duration::from_millis(250),
            "too fast to prove anything"
        );
    }

    #[tokio::test]
    async fn a_non_zero_speed_counts_as_progress() {
        let server = MockServer::start().await;
        let mut moving = info_at("downloading", 7.0);
        moving["speed"] = serde_json::json!(1_000_000);
        let mut seq = vec![moving; 40];
        seq.push(info("downloaded", &["https://real-debrid.com/d/X"]));
        mount_info_sequence(&server, &seq).await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        wait_for_links(&client, "ABC", "all", true, Some(Duration::from_millis(30)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn real_debrid_stages_are_not_stalls() {
        let server = MockServer::start().await;
        let mut seq = vec![info_at("queued", 0.0); 30];
        seq.push(info("downloaded", &["https://real-debrid.com/d/X"]));
        mount_info_sequence(&server, &seq).await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        wait_for_links(&client, "ABC", "all", true, Some(Duration::from_millis(30)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_short_outage_while_waiting_is_ridden_out() {
        let server = MockServer::start().await;
        // Every poll fails for a while (each failed poll is retried by the
        // client, so this is a sustained outage), then the torrent is done.
        Mock::given(method("GET"))
            .and(path("/torrents/info/ABC"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(12)
            .mount(&server)
            .await;
        mount_info_sequence(
            &server,
            &[info("downloaded", &["https://real-debrid.com/d/X"])],
        )
        .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let got = wait_for_links(&client, "ABC", "all", true, None)
            .await
            .unwrap();
        assert_eq!(got.status, "downloaded");

        // ...but not forever, when a stall timeout is set.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/torrents/info/ABC"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = wait_for_links(&client, "ABC", "all", true, Some(Duration::from_millis(1)))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("couldn't reach Real-Debrid"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn add_magnet_checks_after_its_last_attempt_too() {
        let h = "0123456789abcdef0123456789abcdef01234567";
        let server = MockServer::start().await;
        // Listings: empty before and after attempts 1 and 2; the torrent
        // shows up only after the third failed POST.
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(ResponseTemplate::new(204))
            .up_to_n_times(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([listed("LATE", h)])),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(ResponseTemplate::new(503))
            .expect(3)
            .mount(&server)
            .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let id = add(&client, &format!("magnet:?xt=urn:btih:{h}"), None)
            .await
            .unwrap();
        assert_eq!(id, "LATE");
    }

    #[tokio::test]
    async fn magnet_without_a_readable_hash_is_sent_once() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = add(&client, "magnet:?xt=urn:btmh:1220abcdef", None)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("may have been added anyway"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn magnet_with_partial_selection_is_not_reused() {
        let h = "0123456789abcdef0123456789abcdef01234567";
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "id": "PARTIAL", "filename": "f", "hash": h, "bytes": 1, "host": "rd",
                    "split": 2000, "progress": 100.0, "status": "downloaded", "added": "x",
                }])),
            )
            .mount(&server)
            .await;
        let mut partial = info("downloaded", &["https://real-debrid.com/d/X"]);
        partial["files"] = serde_json::json!([
            {"id": 1, "path": "/a", "bytes": 1, "selected": 1},
            {"id": 2, "path": "/b", "bytes": 1, "selected": 0},
        ]);
        Mock::given(method("GET"))
            .and(path("/torrents/info/PARTIAL"))
            .respond_with(ResponseTemplate::new(200).set_body_json(partial))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_body_json(serde_json::json!({"id": "NEW", "uri": "u"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let id = add_or_reuse(
            &client,
            &format!("magnet:?xt=urn:btih:{h}"),
            None,
            "all",
            true,
        )
        .await
        .unwrap();
        assert_eq!(id, "NEW");
    }

    #[tokio::test]
    async fn magnet_reuses_existing_torrent_with_same_hash() {
        let h = "0123456789abcdef0123456789abcdef01234567";
        let server = MockServer::start().await;
        let torrent = |id: &str, status: &str| {
            serde_json::json!({
                "id": id, "filename": "f", "hash": h.to_ascii_uppercase(), "bytes": 1,
                "host": "rd", "split": 2000, "progress": 100.0, "status": status, "added": "x",
            })
        };
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                torrent("FAILED", "error"),
                torrent("GOOD", "downloaded"),
            ])))
            .mount(&server)
            .await;
        let mut good = info("downloaded", &["https://real-debrid.com/d/X"]);
        good["files"] = serde_json::json!([{"id": 1, "path": "/a", "bytes": 1, "selected": 1}]);
        Mock::given(method("GET"))
            .and(path("/torrents/info/GOOD"))
            .respond_with(ResponseTemplate::new(200).set_body_json(good))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/torrents/addMagnet"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let id = add_or_reuse(
            &client,
            &format!("magnet:?xt=urn:btih:{h}"),
            None,
            "all",
            true,
        )
        .await
        .unwrap();
        assert_eq!(id, "GOOD");
    }
}
