use anyhow::Result;

use crate::api::Client;
use crate::api::models::{TrafficDetailsResponse, TrafficResponse};
use crate::config::Config;
use crate::output::{self, human_bytes};

pub async fn show(
    details: bool,
    from: Option<String>,
    to: Option<String>,
    json: bool,
) -> Result<()> {
    let config = Config::load()?;
    let client = Client::new(config.api_token)?;
    client.require_token()?;

    if details {
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(from) = from.as_deref() {
            query.push(("start", from));
        }
        if let Some(to) = to.as_deref() {
            query.push(("end", to));
        }

        let entries: TrafficDetailsResponse =
            client.get_json_query("/traffic/details", &query).await?;
        if json {
            output::print_json(&entries)?;
            return Ok(());
        }

        let mut dates: Vec<_> = entries.iter().collect();
        dates.sort_by(|a, b| a.0.cmp(b.0));

        let rows = dates
            .iter()
            .map(|(date, entry)| vec![(*date).clone(), human_bytes(entry.bytes)])
            .collect();
        output::print_table(&["Date", "Total"], rows);

        let mut host_rows = Vec::new();
        for (date, entry) in &dates {
            let mut hosts: Vec<_> = entry.host.iter().collect();
            hosts.sort_by(|a, b| a.0.cmp(b.0));
            for (host, bytes) in hosts {
                host_rows.push(vec![(*date).clone(), host.clone(), human_bytes(*bytes)]);
            }
        }
        if !host_rows.is_empty() {
            println!();
            output::print_table(&["Date", "Host", "Bytes"], host_rows);
        }
    } else {
        let traffic: TrafficResponse = client.get_json("/traffic").await?;
        if json {
            output::print_json(&traffic)?;
            return Ok(());
        }

        let mut entries: Vec<_> = traffic.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let rows = entries
            .into_iter()
            .map(|(host, entry)| {
                vec![
                    host,
                    human_bytes(entry.bytes.unwrap_or(0)),
                    entry
                        .limit
                        .map(|l| l.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    entry.entry_type,
                    entry.reset.unwrap_or_else(|| "-".to_string()),
                ]
            })
            .collect();

        output::print_table(&["Host", "Used", "Limit", "Type", "Reset"], rows);
    }

    Ok(())
}
