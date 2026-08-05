use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::api::Client;
use crate::api::models::{AddMagnetResponse, TorrentInfo, UnrestrictedLink};
use crate::cli::{DlArgs, UnrestrictArgs};
use crate::config::Config;
use crate::download::{self, DownloadOpts};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const TERMINAL_FAILURE_STATUSES: &[&str] = &["error", "virus", "dead", "magnet_error"];

pub async fn run(args: DlArgs) -> Result<()> {
    if args.link.starts_with("magnet:") || is_torrent_file(&args.link) {
        run_torrent(args).await
    } else {
        // Hoster link, or a file listing several — same code path as `rd unrestrict`.
        let ua = UnrestrictArgs {
            link: args.link,
            output_dir: args.output_dir,
            output_file: None,
            password: args.password,
            test: false,
            quiet: args.quiet,
            list: false,
            json: false,
            concurrency: args.concurrency,
        };
        crate::commands::unrestrict::run(ua).await
    }
}

fn is_torrent_file(link: &str) -> bool {
    Path::new(link).is_file() && link.to_ascii_lowercase().ends_with(".torrent")
}

async fn run_torrent(args: DlArgs) -> Result<()> {
    let config = Config::load()?;
    let client = Client::new(config.api_token)?;
    client.require_token()?;

    let resp: AddMagnetResponse = if args.link.starts_with("magnet:") {
        let mut form: Vec<(&str, &str)> = vec![("magnet", args.link.as_str())];
        if let Some(h) = &args.host {
            form.push(("host", h.as_str()));
        }
        client
            .post_form("/torrents/addMagnet", &form)
            .await
            .context("failed to add magnet")?
    } else {
        let bytes = tokio::fs::read(&args.link)
            .await
            .with_context(|| format!("failed to read torrent file {}", args.link))?;
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(h) = &args.host {
            query.push(("host", h.as_str()));
        }
        client
            .put_bytes("/torrents/addTorrent", &query, bytes)
            .await
            .context("failed to add torrent file")?
    };

    if !args.quiet {
        println!("Added torrent: {}", resp.id);
    }

    client
        .post_form_no_content(
            &format!("/torrents/selectFiles/{}", resp.id),
            &[("files", "all")],
        )
        .await
        .context("failed to select files")?;

    let info = wait_for_links(&client, &resp.id, args.quiet).await?;

    let out_dir = args
        .output_dir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    if args.output_dir.is_some() {
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("failed to create output directory {}", out_dir.display()))?;
    }

    let mut failures = 0usize;
    for link in &info.links {
        match unrestrict_and_download(&client, link, &out_dir, args.quiet, args.concurrency).await {
            Ok(()) => {}
            Err(err) => {
                eprintln!("error: {err:#}");
                failures += 1;
            }
        }
    }

    if failures > 0 {
        anyhow::bail!("{failures} of {} file(s) failed", info.links.len());
    }
    Ok(())
}

async fn unrestrict_and_download(
    client: &Client,
    link: &str,
    out_dir: &Path,
    quiet: bool,
    concurrency: usize,
) -> Result<()> {
    let ul: UnrestrictedLink = client
        .post_form("/unrestrict/link", &[("link", link)])
        .await
        .with_context(|| format!("failed to unrestrict {link}"))?;

    let dest = out_dir.join(&ul.filename);
    if !quiet {
        println!("downloading {} -> {}", ul.filename, dest.display());
    }

    download::fetch(&ul.download, &dest, DownloadOpts { concurrency, quiet })
        .await
        .with_context(|| format!("failed to download {}", ul.filename))
}

/// Polls torrent status until Real-Debrid has resolved download links (or a
/// terminal failure state is reached). Torrents that are already cached
/// resolve on the first check; others may take a while as RD fetches from
/// the swarm — this blocks until then, printing status as it changes.
async fn wait_for_links(client: &Client, id: &str, quiet: bool) -> Result<TorrentInfo> {
    let mut last_status = String::new();
    loop {
        let info: TorrentInfo = client.get_json(&format!("/torrents/info/{id}")).await?;

        if info.status == "downloaded" && !info.links.is_empty() {
            return Ok(info);
        }
        if TERMINAL_FAILURE_STATUSES.contains(&info.status.as_str()) {
            anyhow::bail!("torrent {id} failed with status '{}'", info.status);
        }

        if !quiet && info.status != last_status {
            println!(
                "waiting on torrent: {} ({:.1}%)",
                info.status, info.progress
            );
            last_status = info.status.clone();
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
