use anyhow::Result;

use crate::api::Client;
use crate::api::models::{HostsResponse, HostsStatusResponse};
use crate::config::Config;
use crate::output;

pub async fn list(status: bool, json: bool) -> Result<()> {
    let config = Config::load()?;
    let client = Client::new(config.api_token)?;

    if status {
        let hosts: HostsStatusResponse = client.get_json("/hosts/status").await?;
        if json {
            output::print_json(&hosts)?;
            return Ok(());
        }

        let mut entries: Vec<_> = hosts.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let rows = entries
            .into_iter()
            .map(|(domain, info)| {
                vec![
                    domain,
                    info.name,
                    info.supported.to_string(),
                    info.status,
                    info.check_time,
                ]
            })
            .collect();

        output::print_table(
            &["Domain", "Name", "Supported", "Status", "Check Time"],
            rows,
        );
    } else {
        let hosts: HostsResponse = client.get_json("/hosts").await?;
        if json {
            output::print_json(&hosts)?;
            return Ok(());
        }

        let mut entries: Vec<_> = hosts.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let rows = entries
            .into_iter()
            .map(|(domain, info)| vec![domain, info.name])
            .collect();

        output::print_table(&["Domain", "Name"], rows);
    }

    Ok(())
}
