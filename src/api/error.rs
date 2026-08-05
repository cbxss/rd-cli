use thiserror::Error;

#[derive(Debug, Error)]
pub enum RdError {
    #[error("network error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("real-debrid api error {code}: {message}")]
    Api { code: i32, message: String },

    #[error("not authenticated — run `rd auth login` first")]
    NotAuthenticated,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse response: {0}")]
    Json(#[from] serde_json::Error),

    #[error("rate limited after retries")]
    RateLimited,
}
