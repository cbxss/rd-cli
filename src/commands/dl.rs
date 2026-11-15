//! `rd dl`: one command for magnets, .torrent files, torrent/download IDs,
//! hoster links, and files listing hoster links.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::api::Client;
use crate::api::models::{DownloadEntry, TorrentInfo};
use crate::cli::{DlArgs, UnrestrictArgs};
use crate::download::{self, DownloadOpts};
use crate::torrent::{self, LinkOpts};

pub async fn run(args: DlArgs) -> Result<()> {
    if args.link.starts_with("magnet:") || is_torrent_file(&args.link) {
        return run_torrent(args).await;
    }
    let exists = Path::new(&args.link).exists();
    if args.link.to_ascii_lowercase().ends_with(".torrent") && !exists {
        anyhow::bail!("torrent file not found: {}", args.link);
    }
    if looks_like_id(&args.link) && !exists {
        return run_existing(args).await;
    }

    // Hoster link, or a file listing several — same code path as `rd unrestrict`.
    crate::commands::unrestrict::run(UnrestrictArgs {
        link: args.link,
        output_dir: args.output_dir,
        output_file: None,
        password: args.password,
        test: false,
        quiet: args.quiet,
        list: false,
        json: false,
        concurrency: args.concurrency,
    })
    .await
}

fn is_torrent_file(link: &str) -> bool {
    Path::new(link).is_file() && link.to_ascii_lowercase().ends_with(".torrent")
}

/// Real-Debrid torrent and download IDs are short alphanumeric strings
/// (e.g. `ABCD1234EFGH5`). A hoster link always has a scheme, so there's no
/// ambiguity with those.
fn looks_like_id(s: &str) -> bool {
    (8..=32).contains(&s.len()) && s.chars().all(|c| c.is_ascii_alphanumeric())
}

/// `--stall-timeout 0` means wait forever.
fn stall_timeout(d: Duration) -> Option<Duration> {
    (!d.is_zero()).then_some(d)
}

fn link_opts(args: &DlArgs) -> LinkOpts {
    LinkOpts {
        quiet: args.quiet,
        concurrency: args.concurrency,
        jobs: args.jobs,
    }
}

pub fn prepare_out_dir(output_dir: Option<&str>) -> Result<PathBuf> {
    let Some(dir) = output_dir else {
        return Ok(PathBuf::from("."));
    };
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create output directory {dir}"))?;
    Ok(PathBuf::from(dir))
}

async fn run_torrent(args: DlArgs) -> Result<()> {
    let client = Client::from_config()?;
    let out_dir = prepare_out_dir(args.output_dir.as_deref())?;
    let id = torrent::add_or_reuse(&client, &args.link, args.host.as_deref(), args.quiet).await?;
    torrent::download(
        &client,
        &id,
        "all",
        &out_dir,
        link_opts(&args),
        stall_timeout(args.stall_timeout),
    )
    .await
}

/// `rd dl <id>`: an existing torrent ID (from `rd torrents list`), or a
/// download-history ID (from `rd downloads list`).
async fn run_existing(args: DlArgs) -> Result<()> {
    let client = Client::from_config()?;
    let out_dir = prepare_out_dir(args.output_dir.as_deref())?;

    match client
        .get_json::<TorrentInfo>(&format!("/torrents/info/{}", args.link))
        .await
    {
        Ok(_) => {
            torrent::download(
                &client,
                &args.link,
                "all",
                &out_dir,
                link_opts(&args),
                stall_timeout(args.stall_timeout),
            )
            .await
        }
        Err(err) if torrent::is_not_found(&err) => {
            let downloads: Vec<DownloadEntry> = client.get_all_pages("/downloads", &[]).await?;
            let Some(entry) = downloads.into_iter().find(|d| d.id == args.link) else {
                anyhow::bail!(
                    "'{}' is not a torrent or download ID on this account (see `rd torrents list` / `rd downloads list`)",
                    args.link
                );
            };
            let dest = out_dir.join(download::safe_filename(&entry.filename));
            if !args.quiet {
                download::status(&format!(
                    "downloading {} -> {}",
                    entry.filename,
                    dest.display()
                ));
            }
            let opts = DownloadOpts {
                concurrency: torrent::cap_connections(args.concurrency, entry.chunks),
                quiet: args.quiet,
            };
            download::fetch(&entry.download, &dest, opts)
                .await
                .map(|_| ())
                .with_context(|| format!("failed to download {}", entry.filename))
        }
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_detection() {
        assert!(looks_like_id("ABCD1234EFGH5"));
        assert!(!looks_like_id("https://host.com/file.zip"));
        assert!(!looks_like_id("links.txt"));
    }
}
