//! A stateful fake Real-Debrid API for end-to-end tests: torrents move
//! through their real status lifecycle as they're polled, unrestricted links
//! point at a file server on the same port that honours HTTP ranges, and
//! faults (dropped connections, ignored ranges, slow bodies) can be switched on.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Form, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

pub const TOKEN: &str = "test-token";

#[derive(Clone, Debug)]
pub struct Step {
    pub status: &'static str,
    pub progress: f64,
}

pub fn step(status: &'static str, progress: f64) -> Step {
    Step { status, progress }
}

/// What a torrent looks like when it's created (by `addMagnet` for a known
/// hash, or pre-seeded on the account).
#[derive(Clone)]
pub struct TorrentSpec {
    pub hash: String,
    pub name: String,
    pub files: Vec<(String, Arc<Vec<u8>>)>,
    /// State on creation.
    pub initial: Step,
    /// States taken on each later poll. `waiting_files_selection` holds until
    /// files are selected.
    pub script: Vec<Step>,
    /// Pre-existing file selection; `None` means nothing selected yet.
    pub selected: Option<Vec<bool>>,
}

impl TorrentSpec {
    pub fn new(hash: &str, files: Vec<(&str, Arc<Vec<u8>>)>) -> Self {
        Self {
            hash: hash.to_string(),
            name: "fake-torrent".into(),
            files: files.into_iter().map(|(p, d)| (p.to_string(), d)).collect(),
            initial: step("waiting_files_selection", 0.0),
            script: vec![step("downloaded", 100.0)],
            selected: None,
        }
    }

    pub fn initial(mut self, s: Step) -> Self {
        self.initial = s;
        self
    }

    pub fn script(mut self, s: Vec<Step>) -> Self {
        self.script = s;
        self
    }

    pub fn selected(mut self, sel: Vec<bool>) -> Self {
        self.selected = Some(sel);
        self
    }
}

struct FakeTorrent {
    id: String,
    hash: String,
    name: String,
    files: Vec<(String, Arc<Vec<u8>>, bool)>,
    status: String,
    progress: f64,
    script: VecDeque<Step>,
}

impl FakeTorrent {
    fn apply(&mut self, s: Step) {
        self.status = s.status.to_string();
        self.progress = s.progress;
    }

    fn links(&self, base: &str) -> Vec<String> {
        if self.status != "downloaded" {
            return vec![];
        }
        self.files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.2)
            .map(|(i, _)| format!("{base}/hoster/{}/{}", self.id, i + 1))
            .collect()
    }

    fn json(&self, base: &str) -> Value {
        json!({
            "id": self.id,
            "filename": self.name,
            "hash": self.hash,
            "bytes": self.files.iter().map(|f| f.1.len() as i64).sum::<i64>(),
            "host": "real-debrid.com",
            "split": 2000,
            "progress": self.progress,
            "status": self.status,
            "added": "2026-01-01T00:00:00.000Z",
            "seeders": 3,
            "links": self.links(base),
            "files": self.files.iter().enumerate().map(|(i, f)| json!({
                "id": i + 1, "path": format!("/{}", f.0), "bytes": f.1.len(),
                "selected": if f.2 { 1 } else { 0 },
            })).collect::<Vec<_>>(),
        })
    }
}

#[derive(Default)]
pub struct Faults {
    /// Ignore `Range` headers (except the 1-byte probe) and send the whole body.
    pub ignore_ranges: bool,
    /// Sleep this long per MiB of file body sent.
    pub throttle_ms_per_mib: u64,
    /// Drop the connection without answering on the next N torrent-info polls.
    pub drop_info_requests: u32,
    /// Answer every non-probe range request with this HTTP status (e.g. 403
    /// for a link that expired mid-download).
    pub fail_ranges_with: Option<u16>,
    /// Added to every request, to simulate a real round-trip time.
    pub latency_ms: u64,
    /// Refuse the next N `/unrestrict/link` calls with error 21 ("too many
    /// active downloads").
    pub refuse_unrestricts: u32,
    /// Answer the next N file requests with 403 (after the CLI has already
    /// picked the output name).
    pub refuse_files: u32,
    /// Answer the next N `addMagnet` calls with a 503 *after* creating the
    /// torrent (the server acted, the client can't tell).
    pub add_magnet_5xx_after_create: u32,
}

#[derive(Default)]
struct Inner {
    base: String,
    torrents: Vec<FakeTorrent>,
    templates: HashMap<String, TorrentSpec>,
    blobs: HashMap<String, (String, Arc<Vec<u8>>)>,
    downloads: Vec<Value>,
    next_id: u64,
    faults: Faults,
    add_count: u32,
    premature_selects: u32,
    /// `(start, end)` of every non-probe ranged file request.
    ranges: Vec<(u64, u64)>,
    /// Every file request except the 1-byte probe, ranged or not.
    file_gets: u32,
    unrestricts: u64,
    /// When set, unrestricted links point here (`{base}/{file name}`)
    /// instead of at the fake's own file server.
    download_base: Option<String>,
}

#[derive(Clone)]
pub struct Fake(Arc<Mutex<Inner>>);

impl Fake {
    pub fn start() -> Self {
        let fake = Fake(Arc::new(Mutex::new(Inner::default())));
        let (tx, rx) = std::sync::mpsc::channel();
        let app_state = fake.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(listener.local_addr().unwrap()).unwrap();
                axum::serve(listener, router(app_state)).await.unwrap();
            });
        });
        let addr = rx.recv().unwrap();
        fake.lock().base = format!("http://{addr}");
        fake
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn url(&self) -> String {
        self.lock().base.clone()
    }

    /// `addMagnet` with this spec's hash creates a torrent from it.
    pub fn on_magnet(&self, spec: TorrentSpec) {
        self.lock()
            .templates
            .insert(spec.hash.to_ascii_lowercase(), spec);
    }

    /// Puts a torrent on the account directly; returns its ID.
    pub fn add_torrent(&self, spec: TorrentSpec) -> String {
        create_torrent(&mut self.lock(), spec)
    }

    /// Adds `n` unrelated, finished torrents (for pagination).
    pub fn add_filler_torrents(&self, n: usize) {
        let mut inner = self.lock();
        for i in 0..n {
            let spec = TorrentSpec::new(&format!("{i:040x}"), vec![("f.bin", Arc::new(vec![0u8]))])
                .initial(step("downloaded", 100.0))
                .script(vec![])
                .selected(vec![true]);
            create_torrent(&mut inner, spec);
        }
    }

    /// Adds a download-history entry; returns its ID.
    pub fn add_download(&self, filename: &str, data: Arc<Vec<u8>>) -> String {
        let mut inner = self.lock();
        inner.next_id += 1;
        let id = format!("DL{:011}", inner.next_id);
        let key = format!("dl/{id}");
        let base = inner.base.clone();
        inner
            .blobs
            .insert(key.clone(), (filename.to_string(), data.clone()));
        inner.downloads.push(json!({
            "id": id, "filename": filename, "mimeType": "application/octet-stream",
            "filesize": data.len(), "link": format!("{base}/hoster/x"), "host": "fake",
            "chunks": 32, "download": format!("{base}/files/{key}"),
            "generated": "2026-01-01T00:00:00.000Z",
        }));
        id
    }

    /// Serve file bodies from another server (e.g. an nginx container) that
    /// has each file under its base name.
    pub fn serve_files_from(&self, base: &str) {
        self.lock().download_base = Some(base.trim_end_matches('/').to_string());
    }

    pub fn faults(&self, f: impl FnOnce(&mut Faults)) {
        f(&mut self.lock().faults);
    }

    pub fn add_count(&self) -> u32 {
        self.lock().add_count
    }

    pub fn premature_selects(&self) -> u32 {
        self.lock().premature_selects
    }

    pub fn torrent_count(&self) -> usize {
        self.lock().torrents.len()
    }

    /// Takes (and clears) the count of file requests other than probes.
    pub fn take_file_gets(&self) -> u32 {
        std::mem::take(&mut self.lock().file_gets)
    }

    /// Takes (and clears) the ranged file requests seen so far.
    pub fn take_ranges(&self) -> Vec<(u64, u64)> {
        std::mem::take(&mut self.lock().ranges)
    }
}

fn create_torrent(inner: &mut Inner, spec: TorrentSpec) -> String {
    inner.next_id += 1;
    let id = format!("T{:012}", inner.next_id);
    let sel = spec
        .selected
        .clone()
        .unwrap_or_else(|| vec![false; spec.files.len()]);
    let files = spec
        .files
        .iter()
        .zip(sel)
        .map(|((p, d), s)| (p.clone(), d.clone(), s))
        .collect::<Vec<_>>();
    for (i, (p, d, _)) in files.iter().enumerate() {
        inner
            .blobs
            .insert(format!("{id}/{}", i + 1), (p.clone(), d.clone()));
    }
    let mut t = FakeTorrent {
        id: id.clone(),
        hash: spec.hash.to_ascii_lowercase(),
        name: spec.name,
        files,
        status: String::new(),
        progress: 0.0,
        script: spec.script.into(),
    };
    t.apply(spec.initial);
    inner.torrents.push(t);
    id
}

fn api_error(status: StatusCode, error: &str, code: i32) -> Response {
    (status, Json(json!({"error": error, "error_code": code}))).into_response()
}

fn router(fake: Fake) -> Router {
    let api = Router::new()
        .route("/torrents", get(list_torrents))
        .route("/torrents/info/{id}", get(torrent_info))
        .route("/torrents/addMagnet", post(add_magnet))
        .route("/torrents/selectFiles/{id}", post(select_files))
        .route("/torrents/delete/{id}", delete(delete_torrent))
        .route("/unrestrict/link", post(unrestrict))
        .route("/downloads", get(list_downloads))
        .layer(middleware::from_fn(require_token));
    Router::new()
        .merge(api)
        .route("/files/{a}/{b}", get(serve_file))
        .layer(middleware::from_fn_with_state(fake.clone(), add_latency))
        .with_state(fake)
}

async fn add_latency(State(f): State<Fake>, req: Request, next: Next) -> Response {
    let ms = f.lock().faults.latency_ms;
    if ms > 0 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    next.run(req).await
}

async fn require_token(req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        == Some(&format!("Bearer {TOKEN}"));
    if !ok {
        return api_error(StatusCode::UNAUTHORIZED, "bad_token", 8);
    }
    next.run(req).await
}

fn paginate(items: Vec<Value>, q: &HashMap<String, String>) -> Response {
    let limit: usize = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
    let page: usize = q.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let slice: Vec<Value> = items
        .into_iter()
        .skip((page - 1) * limit)
        .take(limit)
        .collect();
    if slice.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }
    Json(slice).into_response()
}

async fn list_torrents(
    State(f): State<Fake>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let inner = f.lock();
    // Newest first, like Real-Debrid.
    let items = inner
        .torrents
        .iter()
        .rev()
        .map(|t| t.json(&inner.base))
        .collect();
    paginate(items, &q)
}

async fn list_downloads(
    State(f): State<Fake>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let items = f.lock().downloads.iter().rev().cloned().collect();
    paginate(items, &q)
}

async fn torrent_info(State(f): State<Fake>, Path(id): Path<String>) -> Response {
    let drop_it = {
        let mut inner = f.lock();
        let d = inner.faults.drop_info_requests > 0;
        if d {
            inner.faults.drop_info_requests -= 1;
        }
        d
    };
    if drop_it {
        // Unwinding the connection task closes the socket without a response:
        // the client sees "connection closed before message completed".
        panic!("fake: dropping connection on purpose");
    }

    let mut inner = f.lock();
    let base = inner.base.clone();
    let Some(t) = inner.torrents.iter_mut().find(|t| t.id == id) else {
        return api_error(StatusCode::NOT_FOUND, "unknown_ressource", 7);
    };
    let body = t.json(&base);
    if t.status != "waiting_files_selection" {
        if let Some(s) = t.script.pop_front() {
            t.apply(s);
        }
    }
    Json(body).into_response()
}

fn magnet_hash(magnet: &str) -> Option<String> {
    magnet.split(['?', '&']).find_map(|kv| {
        kv.strip_prefix("xt=urn:btih:")
            .map(|h| h.to_ascii_lowercase())
    })
}

async fn add_magnet(State(f): State<Fake>, Form(form): Form<HashMap<String, String>>) -> Response {
    let mut inner = f.lock();
    inner.add_count += 1;
    let Some(hash) = form.get("magnet").and_then(|m| magnet_hash(m)) else {
        return api_error(StatusCode::BAD_REQUEST, "wrong_parameter", 2);
    };
    let spec = inner.templates.get(&hash).cloned().unwrap_or_else(|| {
        // Unknown magnet: never converts.
        TorrentSpec::new(&hash, vec![])
            .initial(step("magnet_conversion", 0.0))
            .script(vec![])
    });
    let id = create_torrent(&mut inner, spec);
    if inner.faults.add_magnet_5xx_after_create > 0 {
        inner.faults.add_magnet_5xx_after_create -= 1;
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", 25);
    }
    let base = inner.base.clone();
    (
        StatusCode::CREATED,
        Json(json!({"id": id, "uri": format!("{base}/torrents/info/{id}")})),
    )
        .into_response()
}

async fn select_files(
    State(f): State<Fake>,
    Path(id): Path<String>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let mut inner = f.lock();
    let Some(idx) = inner.torrents.iter().position(|t| t.id == id) else {
        return api_error(StatusCode::NOT_FOUND, "unknown_ressource", 7);
    };
    match inner.torrents[idx].status.as_str() {
        "waiting_files_selection" => {}
        "magnet_conversion" => {
            inner.premature_selects += 1;
            return api_error(StatusCode::BAD_REQUEST, "wrong_parameter", 2);
        }
        _ => return StatusCode::ACCEPTED.into_response(), // "action already done"
    }
    let files = form.get("files").cloned().unwrap_or_default();
    let t = &mut inner.torrents[idx];
    for (i, file) in t.files.iter_mut().enumerate() {
        file.2 = files == "all" || files.split(',').any(|x| x.trim() == (i + 1).to_string());
    }
    let next = t.script.pop_front().unwrap_or(step("queued", 0.0));
    t.apply(next);
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_torrent(State(f): State<Fake>, Path(id): Path<String>) -> Response {
    f.lock().torrents.retain(|t| t.id != id);
    StatusCode::NO_CONTENT.into_response()
}

async fn unrestrict(State(f): State<Fake>, Form(form): Form<HashMap<String, String>>) -> Response {
    let mut inner = f.lock();
    if inner.faults.refuse_unrestricts > 0 {
        inner.faults.refuse_unrestricts -= 1;
        return api_error(StatusCode::FORBIDDEN, "too_many_active_downloads", 21);
    }
    let base = inner.base.clone();
    let link = form.get("link").cloned().unwrap_or_default();
    let Some(key) = link
        .strip_prefix(&format!("{base}/hoster/"))
        .map(str::to_string)
    else {
        return api_error(StatusCode::BAD_REQUEST, "wrong_parameter", 2);
    };
    let Some((path, data)) = inner.blobs.get(&key).cloned() else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "file_unavailable", 24);
    };
    inner.unrestricts += 1;
    let filename = path.rsplit('/').next().unwrap_or(&path).to_string();
    // A fresh URL every time, as Real-Debrid may do — resume must cope.
    Json(json!({
        "id": format!("U{}", inner.unrestricts), "filename": filename,
        "filesize": data.len(), "link": link, "host": "fake", "chunks": 32,
        "download": match &inner.download_base {
            Some(ext) => format!("{ext}/{filename}"),
            None => format!("{base}/files/{key}?u={}", inner.unrestricts),
        },
        "streamable": 0,
    }))
    .into_response()
}

fn parse_range(h: &HeaderMap) -> Option<(u64, u64)> {
    let v = h
        .get(header::RANGE)?
        .to_str()
        .ok()?
        .strip_prefix("bytes=")?;
    let (s, e) = v.split_once('-')?;
    Some((s.parse().ok()?, e.parse().ok()?))
}

async fn serve_file(
    State(f): State<Fake>,
    Path((a, b)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let (data, ignore_ranges, throttle, fail_with) = {
        let inner = f.lock();
        let Some((_, data)) = inner.blobs.get(&format!("{a}/{b}")).cloned() else {
            return StatusCode::NOT_FOUND.into_response();
        };
        (
            data,
            inner.faults.ignore_ranges,
            inner.faults.throttle_ms_per_mib,
            inner.faults.fail_ranges_with,
        )
    };
    {
        let mut inner = f.lock();
        if inner.faults.refuse_files > 0 {
            inner.faults.refuse_files -= 1;
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let total = data.len() as u64;
    let range = parse_range(&headers).filter(|&(s, e)| s <= e && e < total);
    let is_probe = range == Some((0, 0));
    if !is_probe {
        f.lock().file_gets += 1;
    }
    if let Some(r) = range.filter(|_| !is_probe) {
        f.lock().ranges.push(r);
        if let Some(code) = fail_with {
            return StatusCode::from_u16(code).unwrap().into_response();
        }
    }

    let (status, start, end) = match range {
        Some((s, e)) if is_probe || !ignore_ranges => (StatusCode::PARTIAL_CONTENT, s, e),
        _ => (StatusCode::OK, 0, total.saturating_sub(1)),
    };
    let len = if total == 0 { 0 } else { end - start + 1 };

    let mut resp = Response::builder()
        .status(status)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, len);
    if status == StatusCode::PARTIAL_CONTENT {
        resp = resp.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        );
    }

    const PIECE: u64 = 1024 * 1024;
    let stream = futures_util::stream::unfold(start, move |pos| {
        let data = data.clone();
        async move {
            if len == 0 || pos > end {
                return None;
            }
            let stop = (pos + PIECE).min(end + 1);
            if throttle > 0 {
                tokio::time::sleep(Duration::from_millis(throttle)).await;
            }
            let chunk = Bytes::copy_from_slice(&data[pos as usize..stop as usize]);
            Some((Ok::<_, std::convert::Infallible>(chunk), stop))
        }
    });
    resp.body(Body::from_stream(stream)).unwrap()
}

/// Deterministic, non-repeating test data (so misplaced ranges are caught).
pub fn data(len: usize, seed: u64) -> Arc<Vec<u8>> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    Arc::new(v)
}
