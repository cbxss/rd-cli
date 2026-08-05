use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;

use super::error::RdError;
use super::models::ApiErrorBody;

type Result<T> = std::result::Result<T, RdError>;

const BASE_URL: &str = "https://api.real-debrid.com/rest/1.0";
const MAX_RETRIES: u32 = 5;

pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: Option<String>,
}

enum Body<'a> {
    None,
    Form(&'a [(&'a str, &'a str)]),
    Bytes(Vec<u8>),
}

impl Client {
    pub fn new(token: Option<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("rd/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            http,
            base_url: BASE_URL.to_string(),
            token,
        })
    }

    #[cfg(test)]
    fn with_base_url(base_url: String, token: Option<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("rd/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            http,
            base_url,
            token,
        })
    }

    pub fn require_token(&self) -> Result<&str> {
        self.token.as_deref().ok_or(RdError::NotAuthenticated)
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.get_json_query(path, &[]).await
    }

    pub async fn get_json_query<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self.execute(Method::GET, path, query, Body::None).await?;
        Ok(resp.json::<T>().await?)
    }

    pub async fn post_form<T: DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self
            .execute(Method::POST, path, &[], Body::Form(form))
            .await?;
        Ok(resp.json::<T>().await?)
    }

    pub async fn post_form_no_content(&self, path: &str, form: &[(&str, &str)]) -> Result<()> {
        self.execute(Method::POST, path, &[], Body::Form(form))
            .await?;
        Ok(())
    }

    /// PUT with a raw binary body (e.g. uploading a .torrent file to `/torrents/addTorrent`).
    pub async fn put_bytes<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Result<T> {
        let resp = self
            .execute(Method::PUT, path, query, Body::Bytes(body))
            .await?;
        Ok(resp.json::<T>().await?)
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.execute(Method::DELETE, path, &[], Body::None).await?;
        Ok(())
    }

    /// Executes a request with retry-with-backoff on 429 and transient 5xx errors.
    /// Non-2xx responses are parsed as `ApiErrorBody` and surfaced as `RdError::Api`.
    async fn execute(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Body<'_>,
    ) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempt = 0u32;

        loop {
            let mut req = self.http.request(method.clone(), &url).query(query);
            if let Some(token) = &self.token {
                req = req.bearer_auth(token);
            }
            req = match &body {
                Body::None => req,
                Body::Form(form) => req.form(form),
                Body::Bytes(bytes) => req.body(bytes.clone()),
            };

            let resp = req.send().await?;
            let status = resp.status();

            if status.is_success() {
                return Ok(resp);
            }

            let retryable = status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
            if retryable && attempt < MAX_RETRIES {
                let delay = retry_after(&resp).unwrap_or_else(|| backoff_delay(attempt));
                attempt += 1;
                tokio::time::sleep(delay).await;
                continue;
            }

            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(RdError::RateLimited);
            }

            let body = resp.text().await.unwrap_or_default();
            return Err(match serde_json::from_str::<ApiErrorBody>(&body) {
                Ok(err) => RdError::Api {
                    code: err.error_code.unwrap_or(status.as_u16() as i32),
                    message: err.error,
                },
                Err(_) => RdError::Api {
                    code: status.as_u16() as i32,
                    message: if body.is_empty() {
                        status
                            .canonical_reason()
                            .unwrap_or("request failed")
                            .to_string()
                    } else {
                        body
                    },
                },
            });
        }
    }
}

fn retry_after(resp: &reqwest::Response) -> Option<Duration> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn backoff_delay(attempt: u32) -> Duration {
    let base_ms = 500u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(base_ms.min(10_000))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[derive(Debug, Deserialize)]
    struct Pong {
        pong: bool,
    }

    #[tokio::test]
    async fn no_token_fails_fast_without_a_network_call() {
        let client = Client::with_base_url("http://127.0.0.1:1".to_string(), None).unwrap();
        assert!(matches!(
            client.require_token(),
            Err(RdError::NotAuthenticated)
        ));
    }

    #[tokio::test]
    async fn retries_on_429_then_succeeds() {
        let server = MockServer::start().await;

        // First request: rate limited, but tells us to retry immediately.
        Mock::given(method("GET"))
            .and(path("/ping"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Subsequent requests: succeed.
        Mock::given(method("GET"))
            .and(path("/ping"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"pong": true})),
            )
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let pong: Pong = client.get_json("/ping").await.unwrap();
        assert!(pong.pong);
    }

    #[tokio::test]
    async fn rate_limited_after_exhausting_retries() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/ping"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = client.get_json::<Pong>("/ping").await.unwrap_err();
        assert!(matches!(err, RdError::RateLimited));
    }

    #[tokio::test]
    async fn non_2xx_response_is_mapped_to_api_error() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/ping"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"error": "bad_request", "error_code": 34})),
            )
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let err = client.get_json::<Pong>("/ping").await.unwrap_err();
        match err {
            RdError::Api { code, message } => {
                assert_eq!(code, 34);
                assert_eq!(message, "bad_request");
            }
            other => panic!("expected RdError::Api, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bearer_token_is_sent_when_present() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/ping"))
            .and(wiremock::matchers::header(
                "Authorization",
                "Bearer secret-token",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"pong": true})),
            )
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), Some("secret-token".to_string())).unwrap();
        let pong: Pong = client.get_json("/ping").await.unwrap();
        assert!(pong.pong);
    }
}
