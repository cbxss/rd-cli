use anyhow::Result;

use crate::api::Client;
use crate::api::models::{Torrent, TorrentInfo};
use crate::output;
use crate::torrent::{self, LinkOpts};

fn client() -> Result<Client> {
    Client::from_config()
}

pub async fn add(
    magnet_or_file: String,
    host: Option<String>,
    select: Option<String>,
) -> Result<()> {
    let client = client()?;

    let id = torrent::add(&client, &magnet_or_file, host.as_deref()).await?;
    println!("Added torrent: {id}");

    if let Some(files) = select {
        select_inner(&client, id, files).await?;
    }

    Ok(())
}

pub async fn list(filter: Option<String>, json: bool) -> Result<()> {
    let client = client()?;

    let mut query: Vec<(&str, &str)> = Vec::new();
    if let Some(f) = &filter {
        query.push(("filter", f.as_str()));
    }
    let torrents: Vec<Torrent> = client.get_all_pages("/torrents", &query).await?;

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
    for link in &info.links {
        println!("Link:     {link}");
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
    let (info, selected) = torrent::select_when_ready(
        client,
        &id,
        &files,
        false,
        Some(torrent::DEFAULT_STALL_TIMEOUT),
    )
    .await?;
    if !selected {
        println!(
            "Torrent {id} is '{}', not waiting for a file selection; its selection was not changed. \
             Download it with `rd dl {id} -o <dir>`.",
            info.status
        );
        return Ok(());
    }

    if info.links.is_empty() {
        println!(
            "Torrent status is '{}' — no download links available yet; run `rd dl {id}` to wait for it and download.",
            info.status
        );
        return Ok(());
    }

    let opts = LinkOpts {
        quiet: false,
        concurrency: 8,
        jobs: 3,
    };
    torrent::download_links(client, &info.links, std::path::Path::new("."), opts).await
}

pub async fn delete(id: String) -> Result<()> {
    let client = client()?;
    client.delete(&format!("/torrents/delete/{id}")).await?;
    println!("Deleted torrent {id}");
    Ok(())
}
