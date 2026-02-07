use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use data_encoding::BASE32_NOPAD;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::StatusCode;
use ripget::{download_url_with_progress, ProgressReporter, RipgetError};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_cbor::Value as CborValue;
use sha1::{Digest, Sha1};
use std::collections::VecDeque;
use std::env;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, ReadBuf};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::io::ReaderStream;

const DEFAULT_DOWNLOAD_THREADS: usize = 16;
const LOOKAHEAD_LIMIT: usize = 3;
const DEFAULT_TIP_SCAN_THREADS: usize = 32;
const DEFAULT_BASE_URL: &str = "https://files.old-faithful.net";
const B2_AUTHORIZE_URL: &str = "https://api.backblazeb2.com/b2api/v4/b2_authorize_account";
const MAX_SMALL_FILE_BYTES: u64 = 5_000_000_000;
const MAX_PART_SIZE: u64 = 5_000_000_000;
const MAX_RETRIES: usize = 3;

#[tokio::main]
async fn main() -> Result<()> {
    if LOOKAHEAD_LIMIT == 0 {
        return Err(anyhow!("LOOKAHEAD_LIMIT must be greater than 0"));
    }

    let config = Config::from_env()?;

    let bin_dir = PathBuf::from("bin");
    tokio::fs::create_dir_all(&bin_dir).await?;

    let latest_path = bin_dir.join(".latest");
    let start_epoch = match read_latest(&latest_path)? {
        Some(latest) => latest.saturating_add(1),
        None => config.start_epoch,
    };

    let tip_scan = scan_tip(
        &config.base_url,
        &config.index_base_url,
        &config.network,
        start_epoch,
        config.tip_scan_threads,
    )
    .await?;
    let tip_epoch = tip_scan.tip_epoch;
    if tip_epoch < start_epoch {
        println!(
            "Already at tip (epoch {}). Nothing to do.",
            tip_epoch
        );
        return Ok(());
    }

    println!(
        "Tip scan complete. tip_epoch={} total_bytes={} ({})",
        tip_epoch,
        tip_scan.total_bytes,
        format_bytes(tip_scan.total_bytes)
    );

    let progress = Progress::new(start_epoch, tip_epoch, tip_scan.total_bytes);

    let b2 = Arc::new(B2Client::new(&config.key_id, &config.application_key).await?);
    let (bucket_id, bucket_name) = b2
        .resolve_bucket(config.bucket_name.as_deref())
        .await?;

    progress.ui().println(format!(
        "Starting at epoch {}. Tip epoch {}. Bucket: {}",
        start_epoch,
        tip_epoch,
        bucket_name.clone().unwrap_or_else(|| bucket_id.clone())
    ));

    let mut in_flight: VecDeque<UploadTask> = VecDeque::new();
    let mut epoch = start_epoch;

    loop {
        if in_flight.len() >= LOOKAHEAD_LIMIT {
            await_oldest_upload(&mut in_flight, &latest_path, &progress).await?;
        }

        let outcome = download_epoch(epoch, &config, &bin_dir, &progress).await?;
        match outcome {
            DownloadEpochOutcome::NotFound {
                epoch: missing_epoch,
                resource,
                url,
            } => {
                progress.ui().println(format!(
                    "Got 404 for {} (epoch {}, {}). Assuming tip; stopping.",
                    resource, missing_epoch, url
                ));
                break;
            }
            DownloadEpochOutcome::Ready(files) => {
                log_progress("downloaded", files.epoch, &progress);
                let b2 = b2.clone();
                let bucket_id = bucket_id.clone();
                let progress = progress.clone();
                let handle = tokio::spawn(async move {
                    upload_epoch(b2, &bucket_id, files, &progress).await
                });
                in_flight.push_back(UploadTask { epoch, handle });
                epoch = epoch.saturating_add(1);
            }
        }
    }

    while !in_flight.is_empty() {
        await_oldest_upload(&mut in_flight, &latest_path, &progress).await?;
    }

    progress.ui().println("All done.");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    base_url: String,
    index_base_url: String,
    network: String,
    download_threads: usize,
    tip_scan_threads: usize,
    start_epoch: u64,
    bucket_name: Option<String>,
    key_id: String,
    application_key: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        let base_url = env::var("JETSTREAMER_HTTP_BASE_URL")
            .or_else(|_| env::var("JETSTREAMER_ARCHIVE_BASE"))
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let base_url = trim_trailing_slash(base_url);

        let index_base_url = env::var("JETSTREAMER_COMPACT_INDEX_BASE_URL")
            .or_else(|_| env::var("JETSTREAMER_ARCHIVE_BASE"))
            .unwrap_or_else(|_| base_url.clone());
        let index_base_url = trim_trailing_slash(index_base_url);

        let network = env::var("JETSTREAMER_NETWORK").unwrap_or_else(|_| "mainnet".to_string());

        let download_threads = env::var("DOWNLOAD_THREADS")
            .ok()
            .map(|val| val.parse::<usize>())
            .transpose()
            .context("DOWNLOAD_THREADS must be a valid usize")?
            .unwrap_or(DEFAULT_DOWNLOAD_THREADS);

        if download_threads == 0 {
            return Err(anyhow!("DOWNLOAD_THREADS must be greater than 0"));
        }

        let tip_scan_threads = env::var("TIP_SCAN_THREADS")
            .ok()
            .map(|val| val.parse::<usize>())
            .transpose()
            .context("TIP_SCAN_THREADS must be a valid usize")?
            .unwrap_or(DEFAULT_TIP_SCAN_THREADS);

        if tip_scan_threads == 0 {
            return Err(anyhow!("TIP_SCAN_THREADS must be greater than 0"));
        }

        let start_epoch = env::var("START_EPOCH")
            .ok()
            .map(|val| val.parse::<u64>())
            .transpose()
            .context("START_EPOCH must be a valid u64")?
            .unwrap_or(0);

        let bucket_name = env::var("BACKBLAZE_BUCKET_NAME")
            .ok()
            .or_else(|| env::var("BACKBLAZE_BUCKET").ok());

        let key_id = env::var("BACKBLAZE_KEY_ID")
            .context("BACKBLAZE_KEY_ID is required")?;
        let application_key = env::var("BACKBLAZE_APPLICATION_KEY")
            .context("BACKBLAZE_APPLICATION_KEY is required")?;

        Ok(Self {
            base_url,
            index_base_url,
            network,
            download_threads,
            tip_scan_threads,
            start_epoch,
            bucket_name,
            key_id,
            application_key,
        })
    }
}

#[derive(Clone)]
struct Progress {
    inner: Arc<ProgressInner>,
}

struct ProgressInner {
    downloaded: AtomicU64,
    uploaded: AtomicU64,
    latest_downloaded_epoch: AtomicU64,
    latest_uploaded_epoch: AtomicU64,
    start_epoch: u64,
    tip_epoch: u64,
    ui: ProgressUi,
}

impl Progress {
    fn new(start_epoch: u64, tip_epoch: u64, total_bytes: u64) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                downloaded: AtomicU64::new(0),
                uploaded: AtomicU64::new(0),
                latest_downloaded_epoch: AtomicU64::new(u64::MAX),
                latest_uploaded_epoch: AtomicU64::new(u64::MAX),
                start_epoch,
                tip_epoch,
                ui: ProgressUi::new(total_bytes),
            }),
        }
    }

    fn add_download(&self, bytes: u64) {
        self.inner.downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    fn add_upload(&self, bytes: u64) {
        let new = self.inner.uploaded.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.inner.ui.overall().set_position(new);
    }

    fn mark_downloaded_epoch(&self, epoch: u64) {
        self.inner
            .latest_downloaded_epoch
            .store(epoch, Ordering::Relaxed);
    }

    fn mark_uploaded_epoch(&self, epoch: u64) {
        self.inner
            .latest_uploaded_epoch
            .store(epoch, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.inner.downloaded.load(Ordering::Relaxed),
            self.inner.uploaded.load(Ordering::Relaxed),
        )
    }

    fn latest_epochs(&self) -> (Option<u64>, Option<u64>) {
        let downloaded = self.inner.latest_downloaded_epoch.load(Ordering::Relaxed);
        let uploaded = self.inner.latest_uploaded_epoch.load(Ordering::Relaxed);
        (
            (downloaded != u64::MAX).then_some(downloaded),
            (uploaded != u64::MAX).then_some(uploaded),
        )
    }

    fn remaining_epochs(&self) -> u64 {
        if self.inner.tip_epoch < self.inner.start_epoch {
            return 0;
        }
        match self.latest_epochs().1 {
            Some(uploaded) => self.inner.tip_epoch.saturating_sub(uploaded),
            None => self
                .inner
                .tip_epoch
                .saturating_sub(self.inner.start_epoch)
                .saturating_add(1),
        }
    }

    fn tip_epoch(&self) -> u64 {
        self.inner.tip_epoch
    }


    fn ui(&self) -> &ProgressUi {
        &self.inner.ui
    }
}

struct ProgressUi {
    multi: MultiProgress,
    overall: ProgressBar,
}

impl ProgressUi {
    fn new(total_bytes: u64) -> Self {
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stdout_with_hz(10));

        let overall = multi.add(ProgressBar::new(total_bytes));
        overall.set_style(progress_style());
        overall.set_prefix("overall");
        overall.enable_steady_tick(Duration::from_millis(100));

        Self { multi, overall }
    }

    fn overall(&self) -> &ProgressBar {
        &self.overall
    }

    fn set_overall_message(&self, message: String) {
        self.overall.set_message(message);
    }

    fn start_download(&self, label: &str) -> ProgressBar {
        let bar = self.multi.add(ProgressBar::new(0));
        bar.set_style(progress_style());
        bar.set_prefix("download");
        bar.set_message(label.to_string());
        bar.enable_steady_tick(Duration::from_millis(100));
        bar
    }

    fn start_upload(&self, label: &str, size: u64) -> ProgressBar {
        let bar = self.multi.add(ProgressBar::new(size));
        bar.set_style(progress_style());
        bar.set_prefix("upload");
        bar.set_message(label.to_string());
        bar.enable_steady_tick(Duration::from_millis(100));
        bar
    }

    fn finish_bar(&self, bar: &ProgressBar) {
        bar.finish_and_clear();
        let _ = self.multi.remove(bar);
    }

    fn println(&self, message: impl AsRef<str>) {
        let _ = self.multi.println(message.as_ref());
    }
}

struct UploadProgress {
    bar: ProgressBar,
    committed: AtomicU64,
}

impl UploadProgress {
    fn new(bar: ProgressBar) -> Self {
        Self {
            bar,
            committed: AtomicU64::new(0),
        }
    }

    fn bar(&self) -> &ProgressBar {
        &self.bar
    }

    fn reset_to_committed(&self) {
        let committed = self.committed.load(Ordering::Relaxed);
        self.bar.set_position(committed);
    }

    fn commit(&self, bytes: u64) {
        let new = self.committed.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.bar.set_position(new);
    }
}

struct ProgressReader<R> {
    inner: R,
    bar: ProgressBar,
}

impl<R> ProgressReader<R> {
    fn new(inner: R, bar: ProgressBar) -> Self {
        Self { inner, bar }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let after = buf.filled().len();
            if after > before {
                self.bar.inc((after - before) as u64);
            }
        }
        poll
    }
}

struct RipgetProgress {
    bar: ProgressBar,
}

impl RipgetProgress {
    fn new(bar: ProgressBar) -> Self {
        Self { bar }
    }
}

impl ProgressReporter for RipgetProgress {
    fn init(&self, total: u64) {
        self.bar.set_length(total);
    }

    fn add(&self, delta: u64) {
        self.bar.inc(delta);
    }
}

struct UploadTask {
    epoch: u64,
    handle: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug)]
struct EpochFiles {
    epoch: u64,
    car_path: PathBuf,
    slot_path: PathBuf,
    cid_path: PathBuf,
    car_remote: String,
    slot_remote: String,
    cid_remote: String,
}

enum DownloadEpochOutcome {
    Ready(EpochFiles),
    NotFound {
        epoch: u64,
        resource: &'static str,
        url: String,
    },
}

async fn await_oldest_upload(
    queue: &mut VecDeque<UploadTask>,
    latest_path: &Path,
    progress: &Progress,
) -> Result<()> {
    if let Some(task) = queue.pop_front() {
        let epoch = task.epoch;
        let result = task.handle.await.context("upload task panicked")?;
        result?;
        write_latest(latest_path, epoch)?;
        log_progress("uploaded", epoch, progress);
    }
    Ok(())
}

async fn download_epoch(
    epoch: u64,
    config: &Config,
    bin_dir: &Path,
    progress: &Progress,
) -> Result<DownloadEpochOutcome> {
    let car_name = format!("epoch-{}.car", epoch);
    let car_url = format!("{}/{}/{}", config.base_url, epoch, car_name);
    let car_path = bin_dir.join(&car_name);

    match download_file(
        &car_url,
        &car_path,
        config.download_threads,
        progress.ui(),
        &car_name,
    )
    .await?
    {
        DownloadResult::NotFound => {
            return Ok(DownloadEpochOutcome::NotFound {
                epoch,
                resource: "car",
                url: car_url,
            })
        }
        DownloadResult::Ok(bytes) => {
            progress.add_download(bytes);
        }
    };

    let root = read_car_root_base32(&car_path)
        .with_context(|| format!("failed to read CAR header for epoch {epoch}"))?;

    let slot_name = format!(
        "epoch-{}-{}-{}-slot-to-cid.index",
        epoch, root, config.network
    );
    let slot_url = format!("{}/{}/{}", config.index_base_url, epoch, slot_name);
    let slot_path = bin_dir.join(&slot_name);

    let _slot_bytes = match download_file(
        &slot_url,
        &slot_path,
        config.download_threads,
        progress.ui(),
        &slot_name,
    )
    .await?
    {
        DownloadResult::NotFound => {
            cleanup_partial(&[&car_path, &slot_path]).await?;
            return Ok(DownloadEpochOutcome::NotFound {
                epoch,
                resource: "slot-to-cid index",
                url: slot_url,
            });
        }
        DownloadResult::Ok(bytes) => progress.add_download(bytes),
    };

    let cid_name = format!(
        "epoch-{}-{}-{}-cid-to-offset-and-size.index",
        epoch, root, config.network
    );
    let cid_url = format!("{}/{}/{}", config.index_base_url, epoch, cid_name);
    let cid_path = bin_dir.join(&cid_name);

    let _cid_bytes = match download_file(
        &cid_url,
        &cid_path,
        config.download_threads,
        progress.ui(),
        &cid_name,
    )
    .await?
    {
        DownloadResult::NotFound => {
            cleanup_partial(&[&car_path, &slot_path, &cid_path]).await?;
            return Ok(DownloadEpochOutcome::NotFound {
                epoch,
                resource: "cid-to-offset-and-size index",
                url: cid_url,
            });
        }
        DownloadResult::Ok(bytes) => progress.add_download(bytes),
    };

    let car_remote = format!("{}/{}", epoch, car_name);
    let slot_remote = format!("{}/{}", epoch, slot_name);
    let cid_remote = format!("{}/{}", epoch, cid_name);

    Ok(DownloadEpochOutcome::Ready(EpochFiles {
        epoch,
        car_path,
        slot_path,
        cid_path,
        car_remote,
        slot_remote,
        cid_remote,
    }))
}

async fn download_file(
    url: &str,
    dest: &Path,
    threads: usize,
    ui: &ProgressUi,
    label: &str,
) -> Result<DownloadResult> {
    let bar = ui.start_download(label);
    let reporter = Arc::new(RipgetProgress::new(bar.clone()));
    let result = download_url_with_progress(url, dest, Some(threads), None, Some(reporter), None).await;
    ui.finish_bar(&bar);

    match result {
        Ok(report) => Ok(DownloadResult::Ok(report.bytes)),
        Err(RipgetError::HttpStatus { status, .. }) if status == StatusCode::NOT_FOUND => {
            Ok(DownloadResult::NotFound)
        }
        Err(err) => Err(anyhow!(err)),
    }
}

enum DownloadResult {
    Ok(u64),
    NotFound,
}

async fn upload_epoch(
    b2: Arc<B2Client>,
    bucket_id: &str,
    files: EpochFiles,
    progress: &Progress,
) -> Result<()> {
    progress
        .ui()
        .println(format!("Uploading epoch {}", files.epoch));
    b2.upload_path(bucket_id, &files.car_remote, &files.car_path, progress)
        .await?;
    b2.upload_path(bucket_id, &files.slot_remote, &files.slot_path, progress)
        .await?;
    b2.upload_path(bucket_id, &files.cid_remote, &files.cid_path, progress)
        .await?;
    Ok(())
}

struct B2Client {
    key_id: String,
    application_key: String,
    client: reqwest::Client,
    state: Mutex<B2State>,
}

#[derive(Clone, Debug)]
struct B2State {
    account_id: String,
    api_url: String,
    auth_token: String,
    recommended_part_size: u64,
    absolute_min_part_size: u64,
    allowed_buckets: Option<Vec<AllowedBucket>>,
}

impl B2Client {
    async fn new(key_id: &str, application_key: &str) -> Result<Self> {
        let client = reqwest::Client::new();
        let state = authorize(&client, key_id, application_key).await?;
        Ok(Self {
            key_id: key_id.to_string(),
            application_key: application_key.to_string(),
            client,
            state: Mutex::new(state),
        })
    }

    async fn refresh_authorization(&self) -> Result<()> {
        let state = authorize(&self.client, &self.key_id, &self.application_key).await?;
        let mut guard = self.state.lock().await;
        *guard = state;
        Ok(())
    }

    async fn resolve_bucket(&self, bucket_name: Option<&str>) -> Result<(String, Option<String>)> {
        let state = self.state.lock().await.clone();
        if let Some(bucket_name) = bucket_name {
            if let Some(allowed) = &state.allowed_buckets {
                if let Some(bucket) = allowed
                    .iter()
                    .find(|bucket| bucket.name.as_deref() == Some(bucket_name))
                {
                    return Ok((bucket.id.clone(), bucket.name.clone()));
                }
            }
            let buckets = self.list_buckets(Some(bucket_name)).await?;
            let bucket = buckets
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("bucket not found: {}", bucket_name))?;
            return Ok((bucket.bucket_id, Some(bucket.bucket_name)));
        }

        if let Some(allowed) = state.allowed_buckets {
            if allowed.len() == 1 {
                let bucket = &allowed[0];
                return Ok((bucket.id.clone(), bucket.name.clone()));
            }
        }

        Err(anyhow!(
            "BACKBLAZE_BUCKET_NAME (or BACKBLAZE_BUCKET) is required when multiple buckets are allowed"
        ))
    }

    async fn list_buckets(&self, bucket_name: Option<&str>) -> Result<Vec<Bucket>> {
        let body = {
            let state = self.state.lock().await.clone();
            let mut body = serde_json::json!({"accountId": state.account_id});
            if let Some(name) = bucket_name {
                body["bucketName"] = serde_json::Value::String(name.to_string());
            }
            body
        };
        let response: ListBucketsResponse = self.post_json_with_reauth("b2_list_buckets", body).await?;
        Ok(response.buckets)
    }

    async fn upload_path(
        &self,
        bucket_id: &str,
        file_name: &str,
        path: &Path,
        progress: &Progress,
    ) -> Result<()> {
        let size = fs::metadata(path)
            .with_context(|| format!("unable to stat {}", path.display()))?
            .len();

        let bar = progress.ui().start_upload(file_name, size);
        let upload_progress = UploadProgress::new(bar.clone());
        let result = if size <= MAX_SMALL_FILE_BYTES {
            self.upload_small_file(bucket_id, file_name, path, size, progress, &upload_progress)
                .await
        } else {
            self.upload_large_file(bucket_id, file_name, path, size, progress, &upload_progress)
                .await
        };
        progress.ui().finish_bar(&bar);
        result?;
        Ok(())
    }

    async fn upload_small_file(
        &self,
        bucket_id: &str,
        file_name: &str,
        path: &Path,
        size: u64,
        progress: &Progress,
        upload_progress: &UploadProgress,
    ) -> Result<()> {
        let sha1 = sha1_hex(path).await?;
        let encoded_name = b2_encode_file_name(file_name);

        for attempt in 0..MAX_RETRIES {
            upload_progress.reset_to_committed();
            let upload = self.get_upload_url(bucket_id).await?;
            let file = tokio::fs::File::open(path).await?;
            let reader = ProgressReader::new(file, upload_progress.bar().clone());
            let stream = ReaderStream::new(reader);
            let body = reqwest::Body::wrap_stream(stream);
            let response = self
                .client
                .post(&upload.upload_url)
                .header(AUTHORIZATION, upload.authorization_token)
                .header("X-Bz-File-Name", encoded_name.clone())
                .header("X-Bz-Content-Sha1", sha1.clone())
                .header(CONTENT_TYPE, "b2/x-auto")
                .header(CONTENT_LENGTH, size.to_string())
                .body(body)
                .send()
                .await?;

            if response.status().is_success() {
                upload_progress.commit(size);
                progress.add_upload(size);
                return Ok(());
            }

            if response.status().is_server_error()
                || response.status() == StatusCode::TOO_MANY_REQUESTS
                || response.status() == StatusCode::UNAUTHORIZED
            {
                if attempt + 1 < MAX_RETRIES {
                    continue;
                }
            }

            return Err(parse_b2_error(response).await);
        }

        Err(anyhow!("failed to upload {} after retries", file_name))
    }

    async fn upload_large_file(
        &self,
        bucket_id: &str,
        file_name: &str,
        path: &Path,
        size: u64,
        progress: &Progress,
        upload_progress: &UploadProgress,
    ) -> Result<()> {
        let file_id = self.start_large_file(bucket_id, file_name).await?;
        let (part_size, part_count) = self.compute_part_size(size).await?;

        if part_count > 10_000 {
            return Err(anyhow!(
                "file {} requires {} parts which exceeds the 10,000 part limit",
                file_name,
                part_count
            ));
        }

        let mut part_sha1s = Vec::with_capacity(part_count as usize);

        for part_index in 0..part_count {
            let part_number = (part_index + 1) as u32;
            let offset = part_index * part_size;
            let part_len = std::cmp::min(part_size, size - offset);
            let sha1 = sha1_hex_range(path, offset, part_len).await?;

            let mut uploaded = false;
            for attempt in 0..MAX_RETRIES {
                upload_progress.reset_to_committed();
                let upload = self.get_upload_part_url(&file_id).await?;
                let mut file = tokio::fs::File::open(path).await?;
                file.seek(SeekFrom::Start(offset)).await?;
                let reader = ProgressReader::new(file.take(part_len), upload_progress.bar().clone());
                let stream = ReaderStream::new(reader);
                let body = reqwest::Body::wrap_stream(stream);

                let response = self
                    .client
                    .post(&upload.upload_url)
                    .header(AUTHORIZATION, upload.authorization_token)
                    .header("X-Bz-Part-Number", part_number.to_string())
                    .header("X-Bz-Content-Sha1", sha1.clone())
                    .header(CONTENT_LENGTH, part_len.to_string())
                    .body(body)
                    .send()
                    .await?;

                if response.status().is_success() {
                    upload_progress.commit(part_len);
                    progress.add_upload(part_len);
                    uploaded = true;
                    break;
                }

                if response.status().is_server_error()
                    || response.status() == StatusCode::TOO_MANY_REQUESTS
                    || response.status() == StatusCode::UNAUTHORIZED
                {
                    if attempt + 1 < MAX_RETRIES {
                        continue;
                    }
                }

                return Err(parse_b2_error(response).await);
            }

            if !uploaded {
                return Err(anyhow!(
                    "failed to upload part {} of {}",
                    part_number,
                    file_name
                ));
            }
            part_sha1s.push(sha1);
        }

        self.finish_large_file(&file_id, part_sha1s).await?;
        Ok(())
    }

    async fn compute_part_size(&self, size: u64) -> Result<(u64, u64)> {
        let state = self.state.lock().await.clone();
        let mut part_size = state
            .recommended_part_size
            .max(state.absolute_min_part_size);
        let min_for_parts = (size + 9_999) / 10_000;
        if part_size < min_for_parts {
            part_size = min_for_parts;
        }
        if part_size > MAX_PART_SIZE {
            part_size = MAX_PART_SIZE;
        }
        if part_size < min_for_parts {
            return Err(anyhow!(
                "file size {} requires part size larger than {}",
                size,
                MAX_PART_SIZE
            ));
        }
        let part_count = (size + part_size - 1) / part_size;
        Ok((part_size, part_count))
    }

    async fn get_upload_url(&self, bucket_id: &str) -> Result<UploadUrlResponse> {
        let body = serde_json::json!({"bucketId": bucket_id});
        self.post_json_with_reauth("b2_get_upload_url", body).await
    }

    async fn start_large_file(&self, bucket_id: &str, file_name: &str) -> Result<String> {
        let body = serde_json::json!({
            "bucketId": bucket_id,
            "fileName": file_name,
            "contentType": "b2/x-auto"
        });
        let response: StartLargeFileResponse =
            self.post_json_with_reauth("b2_start_large_file", body).await?;
        Ok(response.file_id)
    }

    async fn get_upload_part_url(&self, file_id: &str) -> Result<UploadPartUrlResponse> {
        let body = serde_json::json!({"fileId": file_id});
        self.post_json_with_reauth("b2_get_upload_part_url", body).await
    }

    async fn finish_large_file(&self, file_id: &str, part_sha1s: Vec<String>) -> Result<()> {
        let body = serde_json::json!({
            "fileId": file_id,
            "partSha1Array": part_sha1s
        });
        let _response: FinishLargeFileResponse =
            self.post_json_with_reauth("b2_finish_large_file", body).await?;
        Ok(())
    }

    async fn post_json_with_reauth<T: DeserializeOwned>(
        &self,
        api_call: &str,
        body: serde_json::Value,
    ) -> Result<T> {
        for attempt in 0..2 {
            let state = self.state.lock().await.clone();
            let url = format!("{}/b2api/v4/{}", state.api_url, api_call);
            let response = self
                .client
                .post(&url)
                .header(AUTHORIZATION, state.auth_token)
                .json(&body)
                .send()
                .await?;

            if response.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                self.refresh_authorization().await?;
                continue;
            }

            if response.status().is_success() {
                return Ok(response.json::<T>().await?);
            }

            return Err(parse_b2_error(response).await);
        }

        Err(anyhow!("B2 API call failed after reauthorization"))
    }
}

async fn authorize(client: &reqwest::Client, key_id: &str, application_key: &str) -> Result<B2State> {
    let auth = STANDARD.encode(format!("{}:{}", key_id, application_key));
    let response = client
        .get(B2_AUTHORIZE_URL)
        .header(AUTHORIZATION, format!("Basic {}", auth))
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(parse_b2_error(response).await);
    }

    let response: AuthorizeResponse = response.json().await?;
    let storage = response.api_info.storage_api;
    Ok(B2State {
        account_id: response.account_id,
        api_url: storage.api_url,
        auth_token: response.authorization_token,
        recommended_part_size: storage.recommended_part_size,
        absolute_min_part_size: storage.absolute_minimum_part_size,
        allowed_buckets: storage.allowed.buckets,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizeResponse {
    account_id: String,
    authorization_token: String,
    api_info: ApiInfo,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiInfo {
    storage_api: StorageApi,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageApi {
    api_url: String,
    recommended_part_size: u64,
    absolute_minimum_part_size: u64,
    allowed: Allowed,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Allowed {
    buckets: Option<Vec<AllowedBucket>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AllowedBucket {
    id: String,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListBucketsResponse {
    buckets: Vec<Bucket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bucket {
    bucket_id: String,
    bucket_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadUrlResponse {
    upload_url: String,
    authorization_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadPartUrlResponse {
    upload_url: String,
    authorization_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartLargeFileResponse {
    file_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FinishLargeFileResponse {
    #[allow(dead_code)]
    file_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct B2ErrorMessage {
    code: String,
    message: String,
    #[allow(dead_code)]
    status: u16,
}

async fn parse_b2_error(response: reqwest::Response) -> anyhow::Error {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if let Ok(message) = serde_json::from_str::<B2ErrorMessage>(&text) {
        anyhow!(
            "B2 error {}: {} ({})",
            status,
            message.code,
            message.message
        )
    } else {
        anyhow!("B2 error {}: {}", status, text)
    }
}

async fn sha1_hex(path: &Path) -> Result<String> {
    let size = fs::metadata(path)?.len();
    sha1_hex_range(path, 0, size).await
}

async fn sha1_hex_range(path: &Path, offset: u64, len: u64) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || sha1_hex_range_sync(&path, offset, len))
        .await?
        .map_err(|err| err)
}

fn sha1_hex_range_sync(path: &Path, offset: u64, len: u64) -> Result<String> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut remaining = len;
    let mut hasher = Sha1::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    while remaining > 0 {
        let to_read = std::cmp::min(remaining, buffer.len() as u64) as usize;
        let read = file.read(&mut buffer[..to_read])?;
        if read == 0 {
            return Err(anyhow!(
                "unexpected EOF while hashing {}",
                path.display()
            ));
        }
        hasher.update(&buffer[..read]);
        remaining = remaining.saturating_sub(read as u64);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_latest(path: &Path) -> Result<Option<u64>> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let trimmed = contents.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                let value = trimmed
                    .parse::<u64>()
                    .context(".latest did not contain a valid u64")?;
                Ok(Some(value))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn write_latest(path: &Path, epoch: u64) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp_path)?;
        use std::io::Write;
        writeln!(file, "{}", epoch)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
}

fn log_progress(stage: &str, epoch: u64, progress: &Progress) {
    match stage {
        "downloaded" => progress.mark_downloaded_epoch(epoch),
        "uploaded" => progress.mark_uploaded_epoch(epoch),
        _ => {}
    }

    let (downloaded, uploaded) = progress.snapshot();
    let (latest_downloaded, latest_uploaded) = progress.latest_epochs();
    let tip_epoch = progress.tip_epoch();
    let remaining_epochs = progress.remaining_epochs();
    let message = format!(
        "dl_epoch={} ul_epoch={} tip={} remaining_epochs={} dl={} ul={}",
        latest_downloaded
            .map(|val| val.to_string())
            .unwrap_or_else(|| "-".to_string()),
        latest_uploaded
            .map(|val| val.to_string())
            .unwrap_or_else(|| "-".to_string()),
        tip_epoch,
        remaining_epochs,
        format_bytes(downloaded),
        format_bytes(uploaded),
    );
    progress.ui().set_overall_message(message);
}

fn format_bytes(bytes: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut size = bytes as f64;
    let mut idx = 0;
    while size >= 1024.0 && idx < units.len() - 1 {
        size /= 1024.0;
        idx += 1;
    }
    format!("{:.2} {}", size, units[idx])
}

fn progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{prefix:>8} {bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta}) {msg}",
    )
    .expect("valid progress bar template")
    .progress_chars("=>-")
}

fn trim_trailing_slash(mut value: String) -> String {
    while value.ends_with('/') {
        value.pop();
    }
    value
}

fn b2_encode_file_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        if is_b2_safe(byte) {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{:02X}", byte));
        }
    }
    out
}

fn is_b2_safe(byte: u8) -> bool {
    matches!(
        byte,
        b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'.'
            | b'_'
            | b'-'
            | b'~'
            | b'/'
            | b'!'
            | b'$'
            | b'\''
            | b'('
            | b')'
            | b'*'
            | b';'
            | b'='
            | b':'
            | b'@'
    )
}

async fn cleanup_partial(paths: &[&Path]) -> Result<()> {
    for path in paths {
        if let Err(err) = tokio::fs::remove_file(path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        }
    }
    Ok(())
}

fn read_car_root_base32(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let header_len = read_uvarint(&mut file)?;
    let header_len = usize::try_from(header_len).context("CAR header too large")?;

    let mut header_bytes = vec![0u8; header_len];
    file.read_exact(&mut header_bytes)?;

    parse_car_root_base32_from_header(&header_bytes)
}

fn parse_car_root_base32_from_header(header_bytes: &[u8]) -> Result<String> {
    let header: CborValue = serde_cbor::from_slice(header_bytes)?;
    let roots = match header {
        CborValue::Map(map) => map
            .into_iter()
            .find_map(|(key, value)| match key {
                CborValue::Text(text) if text == "roots" => Some(value),
                _ => None,
            })
            .ok_or_else(|| anyhow!("CAR header missing roots"))?,
        _ => return Err(anyhow!("CAR header is not a map")),
    };

    let roots = match roots {
        CborValue::Array(values) if !values.is_empty() => values,
        _ => return Err(anyhow!("CAR roots missing or empty")),
    };

    let root = roots.into_iter().next().ok_or_else(|| anyhow!("missing root"))?;
    let cid_bytes = match root {
        CborValue::Bytes(bytes) => bytes,
        CborValue::Tag(tag, boxed) if tag == 42 => match *boxed {
            CborValue::Bytes(bytes) => bytes,
            _ => return Err(anyhow!("unexpected tagged root")),
        },
        _ => return Err(anyhow!("unexpected root format")),
    };

    let mut candidates = Vec::new();
    if cid_bytes.first() == Some(&0) && cid_bytes.len() > 1 {
        candidates.push(cid_bytes[1..].to_vec());
    }
    candidates.push(cid_bytes.clone());

    for candidate in candidates {
        if let Some((version, _)) = read_uvarint_from_slice(&candidate) {
            let cid_v1 = if version == 1 {
                candidate
            } else {
                let mut v = encode_uvarint(1);
                v.extend(encode_uvarint(0x70));
                v.extend(candidate);
                v
            };
            let mut encoded = BASE32_NOPAD.encode(&cid_v1).to_lowercase();
            encoded.insert(0, 'b');
            return Ok(encoded);
        }
    }

    Err(anyhow!("failed to decode CID"))
}

fn read_uvarint<R: Read>(reader: &mut R) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for _ in 0..10 {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        value |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(anyhow!("varint overflow"))
}

fn read_uvarint_from_slice(data: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (idx, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Some((value, idx + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

fn encode_uvarint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
            out.push(byte);
        } else {
            out.push(byte);
            break;
        }
    }
    out
}

struct TipScanResult {
    tip_epoch: u64,
    total_bytes: u64,
}

enum HeadOutcome {
    Ok {
        epoch: u64,
        car_bytes: u64,
        slot_bytes: u64,
        cid_bytes: u64,
        total_bytes: u64,
    },
    NotFound { epoch: u64 },
}

async fn scan_tip(
    base_url: &str,
    index_base_url: &str,
    network: &str,
    start_epoch: u64,
    threads: usize,
) -> Result<TipScanResult> {
    println!(
        "Scanning for tip with {} HEAD workers starting at epoch {}...",
        threads, start_epoch
    );
    let client = reqwest::Client::new();
    let mut join_set = JoinSet::new();
    let mut next_epoch = start_epoch;
    let mut missing_epoch: Option<u64> = None;
    let mut sizes: Vec<(u64, u64)> = Vec::new();
    let mut running_total = 0u64;
    let mut epochs_found = 0u64;

    let spawn_epoch = |epoch: u64, join_set: &mut JoinSet<Result<HeadOutcome>>| {
        let client = client.clone();
        let base_url = base_url.to_string();
        let index_base_url = index_base_url.to_string();
        let network = network.to_string();
        join_set.spawn(async move {
            head_epoch(&client, &base_url, &index_base_url, &network, epoch).await
        });
    };

    while join_set.len() < threads {
        spawn_epoch(next_epoch, &mut join_set);
        next_epoch = next_epoch.saturating_add(1);
    }

    while let Some(result) = join_set.join_next().await {
        let outcome = result.context("tip scan task panicked")??;
        match outcome {
            HeadOutcome::Ok {
                epoch,
                car_bytes,
                slot_bytes,
                cid_bytes,
                total_bytes,
            } => {
                if let Some(missing) = missing_epoch {
                    if epoch >= missing {
                        println!(
                            "Scan epoch {} (after 404 at {}): car={} ({}) slot={} ({}) cid={} ({}) total={} ({}) [ignored]",
                            epoch,
                            missing,
                            car_bytes,
                            format_bytes(car_bytes),
                            slot_bytes,
                            format_bytes(slot_bytes),
                            cid_bytes,
                            format_bytes(cid_bytes),
                            total_bytes,
                            format_bytes(total_bytes)
                        );
                        continue;
                    }
                }

                sizes.push((epoch, total_bytes));
                running_total = running_total.saturating_add(total_bytes);
                epochs_found = epochs_found.saturating_add(1);
                println!(
                    "Scan epoch {}: car={} ({}) slot={} ({}) cid={} ({}) total={} ({}) running_total={} ({}) epochs_found={}",
                    epoch,
                    car_bytes,
                    format_bytes(car_bytes),
                    slot_bytes,
                    format_bytes(slot_bytes),
                    cid_bytes,
                    format_bytes(cid_bytes),
                    total_bytes,
                    format_bytes(total_bytes),
                    running_total,
                    format_bytes(running_total),
                    epochs_found
                );
            }
            HeadOutcome::NotFound { epoch } => {
                missing_epoch = Some(match missing_epoch {
                    Some(existing) => existing.min(epoch),
                    None => epoch,
                });
            }
        }

        if missing_epoch.is_none() {
            spawn_epoch(next_epoch, &mut join_set);
            next_epoch = next_epoch.saturating_add(1);
        }
    }

    let missing = missing_epoch.ok_or_else(|| anyhow!("tip scan did not encounter a 404"))?;
    let tip_epoch = missing.saturating_sub(1);
    let total_bytes = sizes
        .into_iter()
        .filter(|(epoch, _)| *epoch <= tip_epoch)
        .map(|(_, size)| size)
        .sum();

    Ok(TipScanResult {
        tip_epoch,
        total_bytes,
    })
}

async fn head_epoch(
    client: &reqwest::Client,
    base_url: &str,
    index_base_url: &str,
    network: &str,
    epoch: u64,
) -> Result<HeadOutcome> {
    let car_url = format!("{}/{}/epoch-{}.car", base_url, epoch, epoch);
    let car_len = match head_length(client, &car_url).await? {
        Some(len) => len,
        None => return Ok(HeadOutcome::NotFound { epoch }),
    };

    let root = fetch_car_root_base32(client, &car_url)
        .await
        .with_context(|| format!("failed to read CAR header for epoch {}", epoch))?;

    let slot_url = format!(
        "{}/{}/epoch-{}-{}-{}-slot-to-cid.index",
        index_base_url, epoch, epoch, root, network
    );
    let slot_len = match head_length(client, &slot_url).await? {
        Some(len) => len,
        None => return Ok(HeadOutcome::NotFound { epoch }),
    };

    let cid_url = format!(
        "{}/{}/epoch-{}-{}-{}-cid-to-offset-and-size.index",
        index_base_url, epoch, epoch, root, network
    );
    let cid_len = match head_length(client, &cid_url).await? {
        Some(len) => len,
        None => return Ok(HeadOutcome::NotFound { epoch }),
    };

    Ok(HeadOutcome::Ok {
        epoch,
        car_bytes: car_len,
        slot_bytes: slot_len,
        cid_bytes: cid_len,
        total_bytes: car_len.saturating_add(slot_len).saturating_add(cid_len),
    })
}

async fn head_length(client: &reqwest::Client, url: &str) -> Result<Option<u64>> {
    let response = client.head(url).send().await?;
    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT {
        let length = response
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or_else(|| anyhow!("missing Content-Length for {}", url))?
            .to_str()
            .context("Content-Length was not valid UTF-8")?
            .parse::<u64>()
            .context("Content-Length was not a valid u64")?;
        return Ok(Some(length));
    }
    Err(anyhow!("unexpected HTTP {} for {}", status, url))
}

async fn fetch_car_root_base32(client: &reqwest::Client, url: &str) -> Result<String> {
    let initial = fetch_range(client, url, 0, 4095).await?;
    let (header_len, prefix_len) = read_uvarint_from_slice(&initial)
        .ok_or_else(|| anyhow!("failed to parse CAR header length"))?;
    let header_len = usize::try_from(header_len).context("CAR header too large")?;
    let needed = prefix_len + header_len;

    let data = if initial.len() >= needed {
        initial
    } else {
        fetch_range(client, url, 0, needed as u64 - 1).await?
    };

    if data.len() < needed {
        return Err(anyhow!("incomplete CAR header (needed {})", needed));
    }

    let header_bytes = &data[prefix_len..needed];
    parse_car_root_base32_from_header(header_bytes)
}

async fn fetch_range(client: &reqwest::Client, url: &str, start: u64, end: u64) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={}-{}", start, end))
        .send()
        .await?;

    let status = response.status();
    if status != StatusCode::PARTIAL_CONTENT && status != StatusCode::OK {
        return Err(anyhow!("unexpected HTTP {} for {}", status, url));
    }
    Ok(response.bytes().await?.to_vec())
}
