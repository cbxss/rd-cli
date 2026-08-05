use anyhow::Result;

use crate::api::Client;
use crate::api::models::DownloadEntry;
use crate::config::Config;
use crate::output::{self, human_bytes};

pub async fn list(json: bool) -> Result<()> {
    let config = Config::load()?;
    let client = Client::new(config.api_token)?;
    client.require_token()?;

    let downloads: Vec<DownloadEntry> = client.get_json("/downloads").await?;

    if json {
        output::print_json(&downloads)?;
        return Ok(());
    }

    let rows = downloads
        .into_iter()
        .map(|d| {
            vec![
                d.id,
                d.filename,
                human_bytes(d.filesize),
                d.host,
                d.generated,
            ]
        })
        .collect();

    output::print_table(&["ID", "Filename", "Size", "Host", "Generated"], rows);
    Ok(())
}

pub async fn delete(id: String) -> Result<()> {
    let config = Config::load()?;
    let client = Client::new(config.api_token)?;
    client.require_token()?;

    client.delete(&format!("/downloads/delete/{id}")).await?;
    println!("Deleted download {id}");
    Ok(())
}
