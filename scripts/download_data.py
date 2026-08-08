from pathlib import Path
import argparse

import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "yambda"
PARQUET_PATH = DATA_DIR / "flat" / "50m" / "multi_event.parquet"
EXPECTED_ROWS = 47_790_449


def download() -> None:
    hf_hub_download(
        repo_id="yandex/yambda",
        repo_type="dataset",
        filename="flat/50m/multi_event.parquet",
        local_dir=DATA_DIR,
    )


def verify() -> None:
    if not PARQUET_PATH.is_file():
        raise SystemExit(f"Файл не найден: {PARQUET_PATH}")

    rows = pq.ParquetFile(PARQUET_PATH).metadata.num_rows
    if rows != EXPECTED_ROWS:
        raise SystemExit(
            f"Ожидалось {EXPECTED_ROWS:,} строк, найдено {rows:,}"
        )

    print(f"Файл: {PARQUET_PATH}")
    print(f"Размер: {PARQUET_PATH.stat().st_size / 1024**2:.2f} МБ")
    print(f"Строк: {rows:,}")
    print("Проверка пройдена.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Проверить уже загруженный файл без скачивания.",
    )
    args = parser.parse_args()

    if not args.check_only:
        download()
    verify()


if __name__ == "__main__":
    main()
