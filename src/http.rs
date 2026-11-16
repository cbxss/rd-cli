//! HTTP client settings shared by the API client and the downloader.

use std::time::Duration;

use anyhow::{Context, Result};

/// A client builder with the CLI's timeouts and trust settings:
/// - `RD_READ_TIMEOUT_SECS`: how long a connection may go silent (default 60)
/// - `RD_CA_CERT`: path to an extra PEM root certificate to trust (e.g. a
///   TLS-inspecting proxy, or the container tests' private CA)
pub fn builder() -> Result<reqwest::ClientBuilder> {
    let read_timeout = std::env::var("RD_READ_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let mut builder = reqwest::Client::builder()
        .user_agent(concat!("rd/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(read_timeout));
    if let Some(path) = std::env::var_os("RD_CA_CERT").filter(|v| !v.is_empty()) {
        let pem = std::fs::read(&path)
            .with_context(|| format!("failed to read RD_CA_CERT {}", path.to_string_lossy()))?;
        let cert =
            reqwest::Certificate::from_pem(&pem).context("RD_CA_CERT is not a PEM certificate")?;
        builder = builder.add_root_certificate(cert);
    }
    Ok(builder)
}
