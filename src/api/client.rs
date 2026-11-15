use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;

use super::error::RdError;
use super::models::ApiErrorBody;

type Result<T> = std::result::Result<T, RdError>;

const BASE_URL: &str = "https://api.real-debrid.com/rest/1.0";
const MAX_RETRIES: u32 = 5;
/// Page size for list endpoints (the API caps `limit` at 5000; default is 100).
const PAGE_LIMIT: usize = 5000;

pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: Option<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum Retry {
    Always,
    /// Retry only failures where the request certainly wasn't processed
    /// (429, refused connections), never 5xx.
    NotServerErrors,
}

enum Body<'a> {
    None,
    Form(&'a [(&'a str, &'a str)]),
    Bytes(Vec<u8>),
}

impl Client {
    pub fn new(token: Option<String>) -> Result<Self> {
        // RD_API_URL points the CLI at another server (the end-to-end tests'
        // fake Real-Debrid).
        let base = std::env::var("RD_API_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .map(|v| v.trim_end_matches('/').to_string())
            .unwrap_or_else(|| BASE_URL.to_string());
        Self::with_base_url(base, token)
    }

    pub(crate) fn with_base_url(base_url: String, token: Option<String>) -> Result<Self> {
        let http = crate::http::builder()
            .map_err(|e| RdError::Config(format!("{e:#}")))?
            .build()?;
        Ok(Self {
            http,
            base_url,
            token,
        })
    }

    /// A client using the saved token (or `RD_TOKEN`); fails if there is none.
    pub fn from_config() -> anyhow::Result<Self> {
        let config = crate::config::Config::load()?;
        let client = Self::new(config.api_token)?;
        client.require_token()?;
        Ok(client)
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
        let resp = self
            .execute(Method::GET, path, query, Body::None, Retry::Always)
            .await?;
        Ok(resp.json::<T>().await?)
    }

    /// GETs every page of a list endpoint (e.g. `/torrents`). The API returns
    /// 100 entries by default and answers an empty page with `204 No Content`.
    pub async fn get_all_pages<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<Vec<T>> {
        let limit = PAGE_LIMIT.to_string();
        let mut all = Vec::new();
        let mut page = 1usize;
        loop {
            let page_str = page.to_string();
            let mut q: Vec<(&str, &str)> = query.to_vec();
            q.push(("limit", &limit));
            q.push(("page", &page_str));
            let resp = self
                .execute(Method::GET, path, &q, Body::None, Retry::Always)
                .await?;
            let body = resp.bytes().await?;
            if body.iter().all(u8::is_ascii_whitespace) {
                break;
            }
            let items: Vec<T> = serde_json::from_slice(&body)?;
            let n = items.len();
            all.extend(items);
            if n < PAGE_LIMIT {
                break;
            }
            page += 1;
        }
        Ok(all)
    }

    pub async fn post_form<T: DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self
            .execute(Method::POST, path, &[], Body::Form(form), Retry::Always)
            .await?;
        Ok(resp.json::<T>().await?)
    }

    /// POST that is not retried on a 5xx: the server may have acted before
    /// failing (e.g. created a torrent), so the caller must decide.
    pub async fn post_form_once<T: DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self
            .execute(
                Method::POST,
                path,
                &[],
                Body::Form(form),
                Retry::NotServerErrors,
            )
            .await?;
        Ok(resp.json::<T>().await?)
    }

    pub async fn post_form_no_content(&self, path: &str, form: &[(&str, &str)]) -> Result<()> {
        self.execute(Method::POST, path, &[], Body::Form(form), Retry::Always)
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
            .execute(
                Method::PUT,
                path,
                query,
                Body::Bytes(body),
                Retry::NotServerErrors,
            )
            .await?;
        Ok(resp.json::<T>().await?)
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.execute(Method::DELETE, path, &[], Body::None, Retry::Always)
            .await?;
        Ok(())
    }

    /// Executes a request with retry-with-backoff on 429, transient 5xx errors,
    /// and transport errors. Transport errors are retried for every request
    /// when the connection could not be established, but only for idempotent
    /// methods otherwise — a POST that may have reached the server (e.g.
    /// `addMagnet`) must not be replayed blindly.
    /// Non-2xx responses are parsed as `ApiErrorBody` and surfaced as `RdError::Api`.
    async fn execute(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Body<'_>,
        retry: Retry,
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

            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(err) => {
                    let idempotent = matches!(method, Method::GET | Method::DELETE);
                    if (err.is_connect() || idempotent) && attempt < MAX_RETRIES {
                        tokio::time::sleep(backoff_delay(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(err.into());
                }
            };
            let status = resp.status();

            if status.is_success() {
                return Ok(resp);
            }

            let retryable = status == StatusCode::TOO_MANY_REQUESTS
                || (status.is_server_error() && retry == Retry::Always);
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
                    status: status.as_u16(),
                },
                Err(_) => RdError::Api {
                    status: status.as_u16(),
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
            RdError::Api { code, message, .. } => {
                assert_eq!(code, 34);
                assert_eq!(message, "bad_request");
            }
            other => panic!("expected RdError::Api, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_retries_when_connection_is_closed_mid_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // A server that drops the first connection without answering (the
        // "connection closed before message completed" case), then answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            drop(sock);
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = r#"{"pong":true}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let client = Client::with_base_url(format!("http://{addr}"), None).unwrap();
        let pong: Pong = client.get_json("/ping").await.unwrap();
        assert!(pong.pong);
    }

    #[tokio::test]
    async fn get_all_pages_follows_pages_and_treats_204_as_end() {
        use wiremock::matchers::query_param;

        let server = MockServer::start().await;
        let page1: Vec<serde_json::Value> = (0..PAGE_LIMIT).map(|i| serde_json::json!(i)).collect();
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(page1))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([-1])))
            .mount(&server)
            .await;

        let client = Client::with_base_url(server.uri(), None).unwrap();
        let all: Vec<i64> = client.get_all_pages("/torrents", &[]).await.unwrap();
        assert_eq!(all.len(), PAGE_LIMIT + 1);

        let empty = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/torrents"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&empty)
            .await;
        let client = Client::with_base_url(empty.uri(), None).unwrap();
        let none: Vec<i64> = client.get_all_pages("/torrents", &[]).await.unwrap();
        assert!(none.is_empty());
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
