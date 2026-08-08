"""Download and verify the shared Yambda 50M project dataset."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import snapshot_download


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "yambda"
PARQUET_DIR = DATA_DIR / "flat" / "50m"
EXPECTED_FILES = (
    "dislikes.parquet",
    "likes.parquet",
    "listens.parquet",
    "multi_event.parquet",
    "undislikes.parquet",
    "unlikes.parquet",
)
EXPECTED_MULTI_EVENT_ROWS = 47_790_449


def download() -> None:
    snapshot_download(
        repo_id="yandex/yambda",
        repo_type="dataset",
        allow_patterns=["flat/50m/*.parquet"],
        local_dir=DATA_DIR,
    )


def verify() -> None:
    missing = [name for name in EXPECTED_FILES if not (PARQUET_DIR / name).is_file()]
    if missing:
        raise SystemExit(f"Missing dataset files: {', '.join(missing)}")

    total_bytes = sum((PARQUET_DIR / name).stat().st_size for name in EXPECTED_FILES)
    multi_event_rows = pq.ParquetFile(PARQUET_DIR / "multi_event.parquet").metadata.num_rows

    if multi_event_rows != EXPECTED_MULTI_EVENT_ROWS:
        raise SystemExit(
            "Unexpected multi_event row count: "
            f"{multi_event_rows:,} instead of {EXPECTED_MULTI_EVENT_ROWS:,}"
        )

    print(f"Dataset directory: {PARQUET_DIR}")
    print(f"Files: {len(EXPECTED_FILES)}")
    print(f"Size: {total_bytes / 1024**2:.2f} MiB")
    print(f"multi_event rows: {multi_event_rows:,}")
    print("Dataset check passed.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Verify existing files without downloading them.",
    )
    args = parser.parse_args()

    if not args.check_only:
        download()
    verify()


if __name__ == "__main__":
    main()
