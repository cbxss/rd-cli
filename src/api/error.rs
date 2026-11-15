use thiserror::Error;

#[derive(Debug, Error)]
pub enum RdError {
    #[error("network error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("real-debrid api error {code}: {message}{}", hint(*code))]
    Api {
        code: i32,
        message: String,
        /// HTTP status of the response.
        status: u16,
    },

    #[error("not authenticated — run `rd auth login` first")]
    NotAuthenticated,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse response: {0}")]
    Json(#[from] serde_json::Error),

    #[error("rate limited after retries")]
    RateLimited,
}

/// Extra guidance for errors a user can act on.
fn hint(code: i32) -> &'static str {
    match code {
        8 => " — the API token is invalid or expired; run `rd auth login`",
        9 | 20 => " — this needs an active premium account",
        21 => " — too many active downloads on Real-Debrid; wait or delete some torrents",
        23 => " — traffic quota exhausted (see `rd traffic`)",
        34 | 5 => " — rate limited by Real-Debrid; try again in a minute",
        _ => "",
    }
}
