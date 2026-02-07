use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::Bytes;
use data_encoding::BASE32_NOPAD;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CborValue;
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;

const DEFAULT_NUM_THREADS: usize = 16;
const DEFAULT_TIP_SCAN_THREADS: usize = 32;
const DEFAULT_BASE_URL: &str = "https://files.old-faithful.net";
const B2_AUTHORIZE_URL: &str = "https://api.backblazeb2.com/b2api/v4/b2_authorize_account";
const MAX_PART_SIZE: u64 = 5_000_000_000;
const MAX_IN_MEMORY_PART_BYTES: u64 = 256 * 1024 * 1024;
const B2_RETRY_BASE_DELAY_MS: u64 = 1_000;
const B2_RETRY_MAX_DELAY_MS: u64 = 30_000;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::from_env()?);

    let bin_dir = PathBuf::from("bin");
    tokio::fs::create_dir_all(&bin_dir).await?;

    let latest_path = bin_dir.join(".latest");
    let progress_path = bin_dir.join("progress.toml");

    let completed_epochs = read_progress(&progress_path)?;

    let base_start = match read_latest(&latest_path)? {
        Some(latest) => latest.saturating_add(1),
        None => config.start_epoch,
    };
    let mut start_epoch = base_start.max(config.start_epoch);
    while completed_epochs.contains(&start_epoch) {
        start_epoch = start_epoch.saturating_add(1);
    }

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
        println!("Already at tip (epoch {}). Nothing to do.", tip_epoch);
        return Ok(());
    }

    println!(
        "Tip scan complete. tip_epoch={} total_bytes={} ({})",
        tip_epoch,
        tip_scan.total_bytes,
        format_bytes(tip_scan.total_bytes)
    );

    let progress = Progress::new(start_epoch, tip_epoch, tip_scan.total_bytes);

    let completed_bytes = sum_completed_bytes(&tip_scan.epoch_sizes, &completed_epochs);
    if completed_bytes > 0 {
        let max_completed = completed_epochs.iter().copied().max();
        progress.seed_completed(completed_bytes, max_completed);
        progress.ui().println(format!(
            "Resuming with {} bytes already uploaded.",
            format_bytes(completed_bytes)
        ));
    }

    let b2 = Arc::new(B2Client::new(&config.key_id, &config.application_key).await?);
    let (bucket_id, bucket_name) = b2
        .resolve_bucket(config.bucket_name.as_deref())
        .await?;
    let bucket_id = Arc::new(bucket_id);

    progress.ui().println(format!(
        "Starting at epoch {}. Tip epoch {}. Bucket: {}",
        start_epoch,
        tip_epoch,
        bucket_name.clone().unwrap_or_else(|| bucket_id.as_ref().clone())
    ));

    let tracker = Arc::new(Mutex::new(CompletionTracker::new(
        base_start,
        latest_path,
        progress_path,
        completed_epochs,
    )?));
    let download_client = Arc::new(reqwest::Client::new());

    let mut join_set = JoinSet::new();
    let mut next_epoch = start_epoch;
    let mut stop_at: Option<u64> = None;

    while next_epoch <= tip_epoch || !join_set.is_empty() {
        while next_epoch <= tip_epoch
            && join_set.len() < config.num_threads
            && stop_at.map_or(true, |stop| next_epoch < stop)
        {
            let epoch = next_epoch;
            next_epoch = next_epoch.saturating_add(1);

            let already_completed = {
                let guard = tracker.lock().await;
                guard.is_completed(epoch)
            };
            if already_completed {
                progress
                    .ui()
                    .println(format!("Skipping epoch {} (already uploaded).", epoch));
                continue;
            }

            let config = Arc::clone(&config);
            let b2 = Arc::clone(&b2);
            let bucket_id = Arc::clone(&bucket_id);
            let progress = progress.clone();
            let tracker = Arc::clone(&tracker);
            let download_client = Arc::clone(&download_client);

            join_set.spawn(async move {
                process_epoch(
                    epoch,
                    config,
                    download_client,
                    b2,
                    bucket_id,
                    progress,
                    tracker,
                )
                .await
            });
        }

        if let Some(result) = join_set.join_next().await {
            match result.context("epoch task panicked")?? {
                EpochOutcome::Completed(epoch) => {
                    log_progress(epoch, &progress);
                }
                EpochOutcome::NotFound { epoch, resource, url } => {
                    progress.ui().println(format!(
                        "Got 404 for {} (epoch {}, {}). Assuming tip; stopping.",
                        resource, epoch, url
                    ));
                    stop_at = Some(epoch);
                }
            }
        }
    }

    progress.ui().println("All done.");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    base_url: String,
    index_base_url: String,
    network: String,
    num_threads: usize,
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

        let num_threads = env::var("NUM_THREADS")
            .or_else(|_| env::var("DOWNLOAD_THREADS"))
            .ok()
            .map(|val| val.parse::<usize>())
            .transpose()
            .context("NUM_THREADS must be a valid usize")?
            .unwrap_or(DEFAULT_NUM_THREADS);

        if num_threads == 0 {
            return Err(anyhow!("NUM_THREADS must be greater than 0"));
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

        let key_id = env::var("BACKBLAZE_KEY_ID").context("BACKBLAZE_KEY_ID is required")?;
        let application_key =
            env::var("BACKBLAZE_APPLICATION_KEY").context("BACKBLAZE_APPLICATION_KEY is required")?;

        Ok(Self {
            base_url,
            index_base_url,
            network,
            num_threads,
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

    fn seed_completed(&self, bytes: u64, max_epoch: Option<u64>) {
        self.inner.downloaded.store(bytes, Ordering::Relaxed);
        self.inner.uploaded.store(bytes, Ordering::Relaxed);
        self.inner.ui.download().set_position(bytes);
        self.inner.ui.upload().set_position(bytes);
        self.inner.ui.overall().set_position(bytes);
        if let Some(epoch) = max_epoch {
            self.inner
                .latest_downloaded_epoch
                .store(epoch, Ordering::Relaxed);
            self.inner
                .latest_uploaded_epoch
                .store(epoch, Ordering::Relaxed);
        }
    }

    fn add_download(&self, bytes: u64) {
        let new = self.inner.downloaded.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.inner.ui.download().set_position(new);
    }

    fn add_upload(&self, bytes: u64) {
        let new = self.inner.uploaded.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.inner.ui.upload().set_position(new);
        self.inner.ui.overall().set_position(new);
    }

    fn mark_downloaded_epoch(&self, epoch: u64) {
        self.inner
            .latest_downloaded_epoch
            .fetch_max(epoch, Ordering::Relaxed);
    }

    fn mark_uploaded_epoch(&self, epoch: u64) {
        self.inner
            .latest_uploaded_epoch
            .fetch_max(epoch, Ordering::Relaxed);
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
    download: ProgressBar,
    upload: ProgressBar,
}

impl ProgressUi {
    fn new(total_bytes: u64) -> Self {
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stdout_with_hz(10));

        let overall = multi.add(ProgressBar::new(total_bytes));
        overall.set_style(progress_style());
        overall.set_prefix("overall");
        overall.enable_steady_tick(Duration::from_millis(100));

        let download = multi.add(ProgressBar::new(total_bytes));
        download.set_style(progress_style());
        download.set_prefix("download");
        download.enable_steady_tick(Duration::from_millis(100));

        let upload = multi.add(ProgressBar::new(total_bytes));
        upload.set_style(progress_style());
        upload.set_prefix("upload");
        upload.enable_steady_tick(Duration::from_millis(100));

        Self {
            multi,
            overall,
            download,
            upload,
        }
    }

    fn overall(&self) -> &ProgressBar {
        &self.overall
    }

    fn download(&self) -> &ProgressBar {
        &self.download
    }

    fn upload(&self) -> &ProgressBar {
        &self.upload
    }

    fn set_overall_message(&self, message: String) {
        self.overall.set_message(message);
    }

    fn println(&self, message: impl AsRef<str>) {
        let _ = self.multi.println(message.as_ref());
    }
}

struct CompletionTracker {
    next_epoch: u64,
    completed: BTreeSet<u64>,
    latest_path: PathBuf,
    progress_path: PathBuf,
}

impl CompletionTracker {
    fn new(
        start_epoch: u64,
        latest_path: PathBuf,
        progress_path: PathBuf,
        completed: BTreeSet<u64>,
    ) -> Result<Self> {
        let mut tracker = Self {
            next_epoch: start_epoch,
            completed,
            latest_path,
            progress_path,
        };
        tracker.advance_latest()?;
        Ok(tracker)
    }

    fn is_completed(&self, epoch: u64) -> bool {
        self.completed.contains(&epoch)
    }

    fn mark_completed(&mut self, epoch: u64) -> Result<()> {
        if !self.completed.insert(epoch) {
            return Ok(());
        }
        write_progress(&self.progress_path, &self.completed)?;
        self.advance_latest()?;
        Ok(())
    }

    fn advance_latest(&mut self) -> Result<()> {
        while self.completed.contains(&self.next_epoch) {
            write_latest(&self.latest_path, self.next_epoch)?;
            self.next_epoch = self.next_epoch.saturating_add(1);
        }
        Ok(())
    }
}

enum EpochOutcome {
    Completed(u64),
    NotFound {
        epoch: u64,
        resource: &'static str,
        url: String,
    },
}

async fn process_epoch(
    epoch: u64,
    config: Arc<Config>,
    download_client: Arc<reqwest::Client>,
    b2: Arc<B2Client>,
    bucket_id: Arc<String>,
    progress: Progress,
    tracker: Arc<Mutex<CompletionTracker>>, 
) -> Result<EpochOutcome> {
    let car_name = format!("epoch-{}.car", epoch);
    let car_url = format!("{}/{}/{}", config.base_url, epoch, car_name);

    let root = match fetch_car_root_base32(&download_client, &car_url).await {
        Ok(root) => root,
        Err(err) => {
            if is_not_found(&err) {
                return Ok(EpochOutcome::NotFound {
                    epoch,
                    resource: "car",
                    url: car_url,
                });
            }
            return Err(err);
        }
    };

    let car_remote = format!("{}/{}", epoch, car_name);
    match transfer_file(
        &download_client,
        &b2,
        &bucket_id,
        &car_url,
        &car_remote,
        &progress,
    )
    .await?
    {
        TransferOutcome::NotFound => {
            return Ok(EpochOutcome::NotFound {
                epoch,
                resource: "car",
                url: car_url,
            })
        }
        TransferOutcome::Ok => {}
    }

    let slot_name = format!(
        "epoch-{}-{}-{}-slot-to-cid.index",
        epoch, root, config.network
    );
    let slot_url = format!("{}/{}/{}", config.index_base_url, epoch, slot_name);
    let slot_remote = format!("{}/{}", epoch, slot_name);
    match transfer_file(
        &download_client,
        &b2,
        &bucket_id,
        &slot_url,
        &slot_remote,
        &progress,
    )
    .await?
    {
        TransferOutcome::NotFound => {
            return Ok(EpochOutcome::NotFound {
                epoch,
                resource: "slot-to-cid index",
                url: slot_url,
            })
        }
        TransferOutcome::Ok => {}
    }

    let cid_name = format!(
        "epoch-{}-{}-{}-cid-to-offset-and-size.index",
        epoch, root, config.network
    );
    let cid_url = format!("{}/{}/{}", config.index_base_url, epoch, cid_name);
    let cid_remote = format!("{}/{}", epoch, cid_name);
    match transfer_file(
        &download_client,
        &b2,
        &bucket_id,
        &cid_url,
        &cid_remote,
        &progress,
    )
    .await?
    {
        TransferOutcome::NotFound => {
            return Ok(EpochOutcome::NotFound {
                epoch,
                resource: "cid-to-offset-and-size index",
                url: cid_url,
            })
        }
        TransferOutcome::Ok => {}
    }

    {
        let mut guard = tracker.lock().await;
        guard.mark_completed(epoch)?;
    }

    Ok(EpochOutcome::Completed(epoch))
}

enum TransferOutcome {
    Ok,
    NotFound,
}

async fn transfer_file(
    download_client: &reqwest::Client,
    b2: &B2Client,
    bucket_id: &str,
    url: &str,
    remote_name: &str,
    progress: &Progress,
) -> Result<TransferOutcome> {
    let size = match head_length(download_client, url).await? {
        Some(len) => len,
        None => return Ok(TransferOutcome::NotFound),
    };

    b2.upload_large_remote(download_client, bucket_id, remote_name, url, size, progress)
        .await?;
    Ok(TransferOutcome::Ok)
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

    async fn upload_large_remote(
        &self,
        download_client: &reqwest::Client,
        bucket_id: &str,
        file_name: &str,
        url: &str,
        size: u64,
        progress: &Progress,
    ) -> Result<()> {
        let file_id = self.start_large_file(bucket_id, file_name).await?;
        let (part_size, part_count) = self.compute_part_size(size).await?;

        let mut part_sha1s = Vec::with_capacity(part_count as usize);

        for part_index in 0..part_count {
            let part_number = (part_index + 1) as u32;
            let offset = part_index * part_size;
            let part_len = std::cmp::min(part_size, size - offset);
            let end = offset + part_len - 1;
            let part_bytes =
                fetch_range_with_progress(download_client, url, offset, end, progress).await?;
            let sha1 = sha1_hex_bytes(&part_bytes);

            let mut attempt = 0usize;
            loop {
                let upload = self.get_upload_part_url(&file_id).await?;
                let response = self
                    .client
                    .post(&upload.upload_url)
                    .header(AUTHORIZATION, upload.authorization_token)
                    .header("X-Bz-Part-Number", part_number.to_string())
                    .header("X-Bz-Content-Sha1", sha1.clone())
                    .header(CONTENT_LENGTH, part_len.to_string())
                    .body(reqwest::Body::from(part_bytes.clone()))
                    .send()
                    .await;

                let response = match response {
                    Ok(resp) => resp,
                    Err(_) => {
                        sleep_with_backoff(attempt).await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                };

                if response.status().is_success() {
                    progress.add_upload(part_len);
                    break;
                }

                if should_retry_status(response.status())
                    || response.status() == StatusCode::UNAUTHORIZED
                {
                    sleep_with_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                    continue;
                }

                return Err(parse_b2_error(response).await);
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
        if part_size > MAX_IN_MEMORY_PART_BYTES {
            part_size = MAX_IN_MEMORY_PART_BYTES;
        }
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
        let mut attempt = 0usize;
        loop {
            let state = self.state.lock().await.clone();
            let url = format!("{}/b2api/v4/{}", state.api_url, api_call);
            let response = self
                .client
                .post(&url)
                .header(AUTHORIZATION, state.auth_token)
                .json(&body)
                .send()
                .await;

            let response = match response {
                Ok(resp) => resp,
                Err(_) => {
                    sleep_with_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                    continue;
                }
            };

            if response.status().is_success() {
                return Ok(response.json::<T>().await?);
            }

            if response.status() == StatusCode::UNAUTHORIZED {
                self.refresh_authorization().await?;
                sleep_with_backoff(attempt).await;
                attempt = attempt.saturating_add(1);
                continue;
            }

            if should_retry_status(response.status()) {
                sleep_with_backoff(attempt).await;
                attempt = attempt.saturating_add(1);
                continue;
            }

            return Err(parse_b2_error(response).await);
        }
    }
}

async fn authorize(client: &reqwest::Client, key_id: &str, application_key: &str) -> Result<B2State> {
    let auth = STANDARD.encode(format!("{}:{}", key_id, application_key));
    let mut attempt = 0usize;
    loop {
        let response = client
            .get(B2_AUTHORIZE_URL)
            .header(AUTHORIZATION, format!("Basic {}", auth))
            .send()
            .await;

        let response = match response {
            Ok(resp) => resp,
            Err(_) => {
                sleep_with_backoff(attempt).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        if response.status().is_success() {
            let response: AuthorizeResponse = response.json().await?;
            let storage = response.api_info.storage_api;
            return Ok(B2State {
                account_id: response.account_id,
                api_url: storage.api_url,
                auth_token: response.authorization_token,
                recommended_part_size: storage.recommended_part_size,
                absolute_min_part_size: storage.absolute_minimum_part_size,
                allowed_buckets: storage.allowed.buckets,
            });
        }

        if should_retry_status(response.status()) {
            sleep_with_backoff(attempt).await;
            attempt = attempt.saturating_add(1);
            continue;
        }

        return Err(parse_b2_error(response).await);
    }
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

fn sha1_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct ProgressFile {
    epochs: Vec<u64>,
}

fn read_progress(path: &Path) -> Result<BTreeSet<u64>> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            if contents.trim().is_empty() {
                return Ok(BTreeSet::new());
            }
            let progress: ProgressFile = toml::from_str(&contents)
                .context("progress.toml did not contain valid TOML")?;
            Ok(progress.epochs.into_iter().collect())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
        Err(err) => Err(err.into()),
    }
}

fn write_progress(path: &Path, epochs: &BTreeSet<u64>) -> Result<()> {
    let progress = ProgressFile {
        epochs: epochs.iter().copied().collect(),
    };
    let contents = toml::to_string_pretty(&progress)?;
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp_path)?;
        use std::io::Write;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
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

fn sum_completed_bytes(sizes: &BTreeMap<u64, u64>, completed: &BTreeSet<u64>) -> u64 {
    completed
        .iter()
        .filter_map(|epoch| sizes.get(epoch))
        .copied()
        .sum()
}

fn log_progress(epoch: u64, progress: &Progress) {
    progress.mark_downloaded_epoch(epoch);
    progress.mark_uploaded_epoch(epoch);

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

fn should_retry_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}

async fn sleep_with_backoff(attempt: usize) {
    let exp = 1u64 << attempt.min(16) as u32;
    let delay = B2_RETRY_BASE_DELAY_MS.saturating_mul(exp);
    let capped = delay.min(B2_RETRY_MAX_DELAY_MS);
    sleep(Duration::from_millis(capped)).await;
}

fn trim_trailing_slash(mut value: String) -> String {
    while value.ends_with('/') {
        value.pop();
    }
    value
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.to_string().contains("404")
}

struct TipScanResult {
    tip_epoch: u64,
    total_bytes: u64,
    epoch_sizes: BTreeMap<u64, u64>,
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
    let mut epoch_sizes = BTreeMap::new();
    for (epoch, size) in sizes.into_iter() {
        if epoch <= tip_epoch {
            epoch_sizes.insert(epoch, size);
        }
    }
    let total_bytes = epoch_sizes.values().copied().sum();

    Ok(TipScanResult {
        tip_epoch,
        total_bytes,
        epoch_sizes,
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
    let mut attempt = 0usize;
    loop {
        let response = client.head(url).send().await;
        let response = match response {
            Ok(resp) => resp,
            Err(_) => {
                sleep_with_backoff(attempt).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };
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
        if should_retry_status(status) {
            sleep_with_backoff(attempt).await;
            attempt = attempt.saturating_add(1);
            continue;
        }
        return Err(anyhow!("unexpected HTTP {} for {}", status, url));
    }
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
    let mut attempt = 0usize;
    loop {
        let response = client
            .get(url)
            .header(RANGE, format!("bytes={}-{}", start, end))
            .send()
            .await;
        let response = match response {
            Ok(resp) => resp,
            Err(_) => {
                sleep_with_backoff(attempt).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(anyhow!("404 for {}", url));
        }
        if status == StatusCode::PARTIAL_CONTENT || status == StatusCode::OK {
            return Ok(response.bytes().await?.to_vec());
        }
        if should_retry_status(status) {
            sleep_with_backoff(attempt).await;
            attempt = attempt.saturating_add(1);
            continue;
        }
        return Err(anyhow!("unexpected HTTP {} for {}", status, url));
    }
}

async fn fetch_range_with_progress(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    end: u64,
    progress: &Progress,
) -> Result<Bytes> {
    let mut attempt = 0usize;
    loop {
        let response = client
            .get(url)
            .header(RANGE, format!("bytes={}-{}", start, end))
            .send()
            .await;
        let response = match response {
            Ok(resp) => resp,
            Err(_) => {
                sleep_with_backoff(attempt).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(anyhow!("404 for {}", url));
        }
        if status == StatusCode::PARTIAL_CONTENT || status == StatusCode::OK {
            let bytes = response.bytes().await?;
            progress.add_download(bytes.len() as u64);
            return Ok(bytes);
        }
        if should_retry_status(status) {
            sleep_with_backoff(attempt).await;
            attempt = attempt.saturating_add(1);
            continue;
        }
        return Err(anyhow!("unexpected HTTP {} for {}", status, url));
    }
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
