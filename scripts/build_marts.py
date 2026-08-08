from pathlib import Path
import argparse
import json
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mart_settings import DAY_SECONDS, LISTEN_PLUS_THRESHOLD, SESSION_GAP_SECONDS
from src.prepare_events import prepare_events
from src.product_mart import build_product_mart
from src.track_mart import build_track_mart, validate_all_marts
from src.user_mart import build_user_mart


def build_marts(source_path: Path, output_dir: Path, stage_db: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    product_path = output_dir / "mart_product_day.parquet"
    user_path = output_dir / "mart_user.parquet"
    track_path = output_dir / "mart_track.parquet"

    source = prepare_events(source_path, stage_db)
    product = build_product_mart(stage_db, product_path)
    user = build_user_mart(stage_db, user_path)
    track = build_track_mart(stage_db, track_path)
    checks = validate_all_marts(stage_db, product_path, user_path, track_path)

    report = {
        "source": {
            "events": source["events"],
            "listens": source["listens"],
            "users": source["users"],
            "items": source["items"],
        },
        "marts": {
            product_path.name: {"rows": product["rows"], "key": ["day_idx"]},
            user_path.name: {"rows": user["rows"], "key": ["uid"]},
            track_path.name: {"rows": track["rows"], "key": ["item_id"]},
        },
        "checks": checks,
        "definitions": {
            "day_seconds": DAY_SECONDS,
            "session_gap_seconds": SESSION_GAP_SECONDS,
            "listen_plus_threshold": LISTEN_PLUS_THRESHOLD,
        },
    }
    (output_dir / "mart_build_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        type=Path,
        default=PROJECT_ROOT / "data" / "yambda" / "flat" / "50m"
        / "multi_event.parquet",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed",
    )
    parser.add_argument(
        "--stage-db",
        type=Path,
        default=PROJECT_ROOT / "data" / "interim" / "yambda_stage.duckdb",
    )
    args = parser.parse_args()
    print(json.dumps(build_marts(args.source, args.output_dir, args.stage_db), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
