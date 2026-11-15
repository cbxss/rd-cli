use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub email: String,
    pub points: i64,
    pub locale: String,
    pub avatar: String,
    #[serde(rename = "type")]
    pub account_type: String,
    pub premium: i64,
    pub expiration: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiErrorBody {
    pub error: String,
    pub error_code: Option<i32>,
}

// ---- unrestrict ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnrestrictedLink {
    pub id: String,
    pub filename: String,
    #[serde(default)]
    pub filesize: i64,
    #[serde(default)]
    pub link: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub chunks: i64,
    pub download: String,
    #[serde(default)]
    pub streamable: i64,
    #[serde(default)]
    pub alternative: Vec<AlternativeLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternativeLink {
    pub id: String,
    pub filename: String,
    pub download: String,
    #[serde(rename = "type")]
    pub link_type: String,
}

// ---- torrents ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Torrent {
    pub id: String,
    #[serde(default)]
    pub filename: String,
    pub hash: String,
    #[serde(default)]
    pub bytes: i64,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub split: i64,
    #[serde(default)]
    pub progress: f64,
    pub status: String,
    #[serde(default)]
    pub added: String,
    #[serde(default)]
    pub links: Vec<String>,
    #[serde(default)]
    pub ended: Option<String>,
    #[serde(default)]
    pub speed: Option<i64>,
    #[serde(default)]
    pub seeders: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentInfo {
    pub id: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub original_filename: Option<String>,
    pub hash: String,
    #[serde(default)]
    pub bytes: i64,
    #[serde(default)]
    pub original_bytes: Option<i64>,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub split: i64,
    #[serde(default)]
    pub progress: f64,
    pub status: String,
    #[serde(default)]
    pub added: String,
    #[serde(default)]
    pub files: Vec<TorrentFile>,
    #[serde(default)]
    pub links: Vec<String>,
    #[serde(default)]
    pub ended: Option<String>,
    #[serde(default)]
    pub speed: Option<i64>,
    #[serde(default)]
    pub seeders: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentFile {
    pub id: i64,
    pub path: String,
    #[serde(default)]
    pub bytes: i64,
    #[serde(default)]
    pub selected: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddMagnetResponse {
    pub id: String,
    pub uri: String,
}

// ---- hosts ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostInfo {
    pub id: String,
    pub name: String,
    pub image: String,
}

pub type HostsResponse = HashMap<String, HostInfo>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostStatus {
    pub id: String,
    pub name: String,
    pub image: String,
    pub supported: i64,
    pub status: String,
    pub check_time: String,
}

pub type HostsStatusResponse = HashMap<String, HostStatus>;

// ---- traffic ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficEntry {
    #[serde(default)]
    pub left: Option<i64>,
    #[serde(default)]
    pub bytes: Option<i64>,
    #[serde(default)]
    pub links: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(rename = "type")]
    pub entry_type: String,
    #[serde(default)]
    pub extra: Option<i64>,
    #[serde(default)]
    pub reset: Option<String>,
}

pub type TrafficResponse = HashMap<String, TrafficEntry>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficDetailsEntry {
    #[serde(default)]
    pub host: HashMap<String, i64>,
    pub bytes: i64,
}

pub type TrafficDetailsResponse = HashMap<String, TrafficDetailsEntry>;

// ---- downloads ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadEntry {
    pub id: String,
    pub filename: String,
    #[serde(default, rename = "mimeType")]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub filesize: i64,
    #[serde(default)]
    pub link: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub chunks: i64,
    pub download: String,
    #[serde(default)]
    pub generated: String,
    #[serde(default)]
    pub streamable: Option<i64>,
}
