from pathlib import Path

from src.mart_settings import (
    DAY_SECONDS,
    LISTEN_PLUS_THRESHOLD,
    SESSION_GAP_SECONDS,
    connect,
)
from src.quality_checks import source_profile, sql_path


def prepare_events(source_path: Path, database_path: Path) -> dict[str, int | float]:
    if not source_path.is_file():
        raise FileNotFoundError(
            f"Source data not found: {source_path}. Run scripts/download_data.py first."
        )

    profile = source_profile(source_path)
    if profile["required_nulls"] != 0 or profile["invalid_event_type"] != 0:
        raise ValueError(f"Source quality checks failed: {profile}")

    connection = connect(database_path)
    source = sql_path(source_path)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE stage_events AS
        WITH ordered AS (
            SELECT
                *,
                lag(timestamp) OVER (
                    PARTITION BY uid
                    ORDER BY timestamp, item_id, event_type
                ) AS previous_timestamp
            FROM read_parquet('{source}')
        )
        SELECT
            uid,
            item_id,
            timestamp,
            timestamp // {DAY_SECONDS} AS day_idx,
            is_organic,
            event_type,
            played_ratio_pct,
            track_length_seconds,
            event_type = 'listen' AS is_listen,
            event_type = 'listen' AND played_ratio_pct > {LISTEN_PLUS_THRESHOLD}
                AS is_listen_plus,
            event_type = 'listen' AND played_ratio_pct > 100 AS is_replay,
            event_type = 'listen' AND is_organic = 0 AS is_recommendation_listen,
            CASE
                WHEN event_type = 'listen'
                THEN track_length_seconds * played_ratio_pct / 100.0
            END AS play_seconds,
            previous_timestamp IS NULL
                OR timestamp - previous_timestamp > {SESSION_GAP_SECONDS}
                AS is_session_start
        FROM ordered
        """
    )
    stage_rows = connection.execute("SELECT count(*) FROM stage_events").fetchone()[0]
    connection.close()
    if stage_rows != profile["events"]:
        raise RuntimeError("Prepared events do not match the source row count")
    return profile
