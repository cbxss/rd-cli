//! `rd dl`: one command for magnets, .torrent files, torrent/download IDs,
//! hoster links, and files listing hoster links.

use std::path::Path;
use std::time::Duration;

use anyhow::Result;

use crate::api::Client;
use crate::api::models::{DownloadEntry, TorrentInfo};
use crate::cli::{DlArgs, UnrestrictArgs};
use crate::commands::unrestrict;
use crate::download::{self, DownloadOpts, Report};
use crate::torrent::{self, LinkOpts};

pub async fn run(mut args: DlArgs) -> Result<()> {
    // Machine-readable modes keep stdout for their own output.
    args.quiet |= args.json || args.list;

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
    if args.select.is_some() {
        anyhow::bail!("--select only applies to magnets, .torrent files and torrent ids");
    }

    // Hoster link, or a file listing several — same code path as `rd unrestrict`.
    let unrestrict_args = UnrestrictArgs {
        link: args.link,
        output_dir: args.output_dir,
        output_file: args.output_file,
        password: args.password,
        test: false,
        quiet: args.quiet,
        list: args.list,
        json: false,
        concurrency: args.concurrency,
    };
    let client = Client::from_config()?;
    let resolved = unrestrict::resolve(&client, &unrestrict_args).await?;
    if args.list {
        for url in resolved.direct_urls() {
            println!("{url}");
        }
        return resolved.finish_without_download();
    }
    unrestrict::download_resolved(resolved, &unrestrict_args, args.quiet)
        .await?
        .finish(args.json)
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

async fn run_torrent(args: DlArgs) -> Result<()> {
    let client = Client::from_config()?;
    let files = args.select.as_deref().unwrap_or("all");
    let id =
        torrent::add_or_reuse(&client, &args.link, args.host.as_deref(), files, args.quiet).await?;
    torrent_output(&client, &id, files, &args).await
}

/// Lists or downloads torrent `id`'s files.
async fn torrent_output(client: &Client, id: &str, files: &str, args: &DlArgs) -> Result<()> {
    let stall = stall_timeout(args.stall_timeout);
    if args.list {
        for url in torrent::direct_links(client, id, files, args.quiet, stall).await? {
            println!("{url}");
        }
        return Ok(());
    }
    let out_dir = download::prepare_out_dir(args.output_dir.as_deref())?;
    torrent::download(
        client,
        id,
        files,
        &out_dir,
        link_opts(args),
        stall,
        args.output_file.as_deref(),
    )
    .await?
    .finish(args.json)
}

/// `rd dl <id>`: an existing torrent ID (from `rd torrents list`), or a
/// download-history ID (from `rd downloads list`).
async fn run_existing(args: DlArgs) -> Result<()> {
    let client = Client::from_config()?;

    match client
        .get_json::<TorrentInfo>(&format!("/torrents/info/{}", args.link))
        .await
    {
        Ok(_) => {
            // Without --select, download whatever is selected (all files if
            // nothing is yet): `rd torrents select` may have picked them.
            let files = match args.select.as_deref() {
                Some(files) => {
                    torrent::ensure_selection(&client, &args.link, files).await?;
                    files
                }
                None => "all",
            };
            torrent_output(&client, &args.link, files, &args).await
        }
        Err(err) if torrent::is_not_found(&err) => {
            let downloads: Vec<DownloadEntry> = client.get_all_pages("/downloads", &[]).await?;
            let Some(entry) = downloads.into_iter().find(|d| d.id == args.link) else {
                anyhow::bail!(
                    "'{}' is not a torrent or download ID on this account (see `rd torrents list` / `rd downloads list`)",
                    args.link
                );
            };
            if args.select.is_some() {
                anyhow::bail!(
                    "--select only applies to torrents; {} is a download",
                    args.link
                );
            }
            if args.list {
                println!("{}", entry.download);
                return Ok(());
            }
            let out_dir = download::prepare_out_dir(args.output_dir.as_deref())?;
            let name = match &args.output_file {
                Some(name) => name.clone(),
                None => download::safe_filename(&entry.filename),
            };
            let dest = out_dir.join(name);
            let opts = DownloadOpts {
                concurrency: args.concurrency,
                quiet: args.quiet,
            };
            let mut report = Report::default();
            match download::fetch_file(&entry.download, &entry.filename, entry.chunks, &dest, opts)
                .await
            {
                Ok(path) => report.add_file(&path),
                Err(err) => {
                    eprintln!("error: {err:#}");
                    report.add_failure(&entry.link, &err);
                }
            }
            report.finish(args.json)
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
