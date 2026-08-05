use std::env;

use anyhow::{Context, Result};

use crate::api::Client;
use crate::api::models::{AddMagnetResponse, Torrent, TorrentInfo, UnrestrictedLink};
use crate::config::Config;
use crate::download::{self, DownloadOpts};
use crate::output;

fn client() -> Result<Client> {
    let config = Config::load()?;
    let client = Client::new(config.api_token)?;
    client.require_token()?;
    Ok(client)
}

pub async fn add(
    magnet_or_file: String,
    host: Option<String>,
    select: Option<String>,
) -> Result<()> {
    let client = client()?;

    let resp: AddMagnetResponse = if magnet_or_file.starts_with("magnet:") {
        let mut form: Vec<(&str, &str)> = vec![("magnet", magnet_or_file.as_str())];
        if let Some(h) = &host {
            form.push(("host", h.as_str()));
        }
        client
            .post_form("/torrents/addMagnet", &form)
            .await
            .context("failed to add magnet")?
    } else {
        let bytes = tokio::fs::read(&magnet_or_file)
            .await
            .with_context(|| format!("failed to read torrent file {magnet_or_file}"))?;
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(h) = &host {
            query.push(("host", h.as_str()));
        }
        client
            .put_bytes("/torrents/addTorrent", &query, bytes)
            .await
            .context("failed to add torrent file")?
    };

    println!("Added torrent: {}", resp.id);

    if let Some(files) = select {
        select_inner(&client, resp.id, files).await?;
    }

    Ok(())
}

pub async fn list(filter: Option<String>, json: bool) -> Result<()> {
    let client = client()?;

    let mut query: Vec<(&str, &str)> = Vec::new();
    if let Some(f) = &filter {
        query.push(("filter", f.as_str()));
    }
    let torrents: Vec<Torrent> = client.get_json_query("/torrents", &query).await?;

    if json {
        output::print_json(&torrents)?;
        return Ok(());
    }

    let rows = torrents
        .into_iter()
        .map(|t| {
            vec![
                t.id,
                t.filename,
                t.status,
                output::human_bytes(t.bytes),
                format!("{:.1}%", t.progress),
            ]
        })
        .collect();
    output::print_table(&["ID", "FILENAME", "STATUS", "SIZE", "PROGRESS"], rows);
    Ok(())
}

pub async fn info(id: String) -> Result<()> {
    let client = client()?;
    let info: TorrentInfo = client.get_json(&format!("/torrents/info/{id}")).await?;

    println!("ID:       {}", info.id);
    println!("Filename: {}", info.filename);
    println!("Hash:     {}", info.hash);
    println!("Status:   {}", info.status);
    println!("Size:     {}", output::human_bytes(info.bytes));
    println!("Progress: {:.1}%", info.progress);
    println!("Host:     {}", info.host);
    if let Some(speed) = info.speed {
        println!("Speed:    {}/s", output::human_bytes(speed));
    }
    if let Some(seeders) = info.seeders {
        println!("Seeders:  {seeders}");
    }
    println!("Added:    {}", info.added);
    if let Some(ended) = &info.ended {
        println!("Ended:    {ended}");
    }
    if !info.links.is_empty() {
        println!("Links:    {}", info.links.len());
    }

    if !info.files.is_empty() {
        println!();
        let rows = info
            .files
            .into_iter()
            .map(|f| {
                vec![
                    f.id.to_string(),
                    f.path,
                    output::human_bytes(f.bytes),
                    if f.selected != 0 {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    },
                ]
            })
            .collect();
        output::print_table(&["ID", "PATH", "SIZE", "SELECTED"], rows);
    }

    Ok(())
}

pub async fn select(id: String, files: String) -> Result<()> {
    let client = client()?;
    select_inner(&client, id, files).await
}

async fn select_inner(client: &Client, id: String, files: String) -> Result<()> {
    client
        .post_form_no_content(
            &format!("/torrents/selectFiles/{id}"),
            &[("files", files.as_str())],
        )
        .await
        .context("failed to select files")?;

    let info: TorrentInfo = client.get_json(&format!("/torrents/info/{id}")).await?;

    if info.links.is_empty() {
        println!(
            "Files selected. Torrent status is '{}' — no download links available yet; check back with `rd torrents info {id}` or `rd torrents select {id} {files}` once it finishes.",
            info.status
        );
        return Ok(());
    }

    let dest_dir = env::current_dir().context("failed to determine current directory")?;

    for link in &info.links {
        let resolved: UnrestrictedLink = client
            .post_form("/unrestrict/link", &[("link", link.as_str())])
            .await
            .with_context(|| format!("failed to unrestrict link {link}"))?;

        let dest = dest_dir.join(&resolved.filename);
        println!("Downloading {}...", resolved.filename);
        download::fetch(&resolved.download, &dest, DownloadOpts::default())
            .await
            .with_context(|| format!("failed to download {}", resolved.filename))?;
    }

    Ok(())
}

pub async fn delete(id: String) -> Result<()> {
    let client = client()?;
    client.delete(&format!("/torrents/delete/{id}")).await?;
    println!("Deleted torrent {id}");
    Ok(())
}
