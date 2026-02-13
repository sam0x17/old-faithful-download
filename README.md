# Old Faithful Downloader

Downloads Old Faithful archive epochs (CAR + index files) and streams them directly into Backblaze B2, with resumable progress and aggregate progress bars.

## What It Does
- Scans for the tip by issuing parallel `HEAD` requests (and a small range GET for CAR headers).
- Computes total bytes up front for an exact ETA.
- Streams downloads directly into B2 multipart uploads (no local file writes).
- Tracks completed epochs in `bin/progress.toml` for idempotent resume.
- Advances `bin/.latest` for contiguous completed epochs.

## Requirements
- Rust toolchain (stable).
- Backblaze B2 credentials in env.

## Env Vars
Required:
- `BACKBLAZE_KEY_ID`
- `BACKBLAZE_APPLICATION_KEY`
- `BACKBLAZE_BUCKET_NAME` (or `BACKBLAZE_BUCKET`)

Optional:
- `NUM_THREADS` (default: 16)  
  Controls both concurrent downloads and uploads.
- `CHECK_THREADS` (default: 16)  
  Concurrency for `--check` mode.
- `TIP_SCAN_THREADS` (default: 32)  
  Parallelism for tip scan HEAD requests.
- `START_EPOCH` (default: 0)
- `JETSTREAMER_HTTP_BASE_URL` (default: `https://files.old-faithful.net`)
- `JETSTREAMER_COMPACT_INDEX_BASE_URL` (default: same as base)
- `JETSTREAMER_NETWORK` (default: `mainnet`)

## Usage
```bash
export BACKBLAZE_KEY_ID=...
export BACKBLAZE_APPLICATION_KEY=...
export BACKBLAZE_BUCKET_NAME=...

cargo run --release
```

## Check Mode
Verify that each epoch’s files in Backblaze match the source file sizes:
```bash
cargo run --release -- --check
```

## Progress & Resume
- Uses three aggregate progress bars:
  - Overall progress (download + upload)
  - Cumulative download speed
  - Cumulative upload speed
- `bin/progress.toml` records fully uploaded epochs.
- `bin/.latest` records the highest contiguous completed epoch.
- Resume is automatic and idempotent: completed epochs are skipped.

## Files
- `bin/.keep` keeps the `bin` directory tracked in git.
- `bin/progress.toml` contains completed epochs.
- `bin/.latest` is updated atomically.
