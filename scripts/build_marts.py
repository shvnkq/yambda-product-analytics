from pathlib import Path
import argparse
import json
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mart_settings import (
    DAY_SECONDS,
    LISTEN_PLUS_THRESHOLD,
    SESSION_GAP_SECONDS,
    TRACK_MIN_LISTENS,
)
from src.prepare_events import prepare_events, source_profile
from src.product_mart import build_product_mart, build_recommendation_funnel
from src.track_mart import build_track_mart, validate_all_marts
from src.user_mart import build_retention_mart, build_user_day_mart, build_user_mart


def build_marts(
    source_path: Path,
    output_dir: Path,
    stage_db: Path,
    *,
    reuse_stage: bool = False,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    product_path = output_dir / "mart_product_day.parquet"
    user_path = output_dir / "mart_user.parquet"
    user_day_path = output_dir / "mart_user_day.parquet"
    retention_path = output_dir / "mart_retention_cohort.parquet"
    track_path = output_dir / "mart_track.parquet"
    funnel_path = output_dir / "mart_recommendation_funnel.parquet"

    if reuse_stage:
        if not stage_db.is_file():
            raise FileNotFoundError(f"Промежуточная база не найдена: {stage_db}")
        source = source_profile(source_path)
    else:
        source = prepare_events(source_path, stage_db)
    product = build_product_mart(stage_db, product_path)
    funnel = build_recommendation_funnel(stage_db, funnel_path)
    user = build_user_mart(stage_db, user_path)
    user_day = build_user_day_mart(stage_db, user_day_path)
    retention = build_retention_mart(stage_db, retention_path)
    track = build_track_mart(stage_db, track_path)
    checks = validate_all_marts(
        stage_db,
        product_path,
        user_path,
        user_day_path,
        retention_path,
        track_path,
        funnel_path,
    )

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
            user_day_path.name: {
                "rows": user_day["rows"],
                "key": ["uid", "day_idx"],
            },
            retention_path.name: {
                "rows": retention["rows"],
                "key": [
                    "cohort_day",
                    "lifetime_day",
                    "recommendation_bucket",
                    "engagement_bucket",
                ],
            },
            track_path.name: {"rows": track["rows"], "key": ["item_id"]},
            funnel_path.name: {"rows": funnel["rows"], "key": ["step_order"]},
        },
        "checks": checks,
        "definitions": {
            "day_seconds": DAY_SECONDS,
            "session_gap_seconds": SESSION_GAP_SECONDS,
            "listen_plus_threshold": LISTEN_PLUS_THRESHOLD,
            "track_min_listens": TRACK_MIN_LISTENS,
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
    parser.add_argument(
        "--reuse-stage",
        action="store_true",
        help="Использовать уже подготовленную stage_events без перезаписи DuckDB",
    )
    args = parser.parse_args()
    print(
        json.dumps(
            build_marts(
                args.source,
                args.output_dir,
                args.stage_db,
                reuse_stage=args.reuse_stage,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
