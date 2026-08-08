from pathlib import Path

import duckdb

from src.mart_settings import (
    DAY_SECONDS,
    LISTEN_PLUS_THRESHOLD,
    SESSION_GAP_SECONDS,
    connect,
    sql_path,
)


EXPECTED_EVENTS = ("listen", "like", "dislike", "unlike", "undislike")


def source_profile(source_path: Path) -> dict[str, int | float]:
    source = sql_path(source_path)
    connection = duckdb.connect()
    row = connection.execute(
        f"""
        SELECT
            count(*) AS events,
            count(DISTINCT uid) AS users,
            count(DISTINCT item_id) AS items,
            min(timestamp) AS min_timestamp,
            max(timestamp) AS max_timestamp,
            sum(timestamp % 5 <> 0) AS timestamps_not_multiple_of_5,
            sum(uid IS NULL OR item_id IS NULL OR timestamp IS NULL
                OR is_organic IS NULL OR event_type IS NULL) AS required_nulls,
            sum(is_organic NOT IN (0, 1)) AS invalid_is_organic,
            sum(event_type NOT IN {EXPECTED_EVENTS}) AS invalid_event_type,
            sum(event_type = 'listen') AS listens,
            sum(event_type <> 'listen' AND
                (played_ratio_pct IS NOT NULL OR track_length_seconds IS NOT NULL))
                AS non_listen_with_play_fields
        FROM read_parquet('{source}')
        """
    ).fetchone()
    columns = [column[0] for column in connection.description]
    connection.close()
    return dict(zip(columns, row, strict=True))


def prepare_events(source_path: Path, database_path: Path) -> dict[str, int | float]:
    if not source_path.is_file():
        raise FileNotFoundError(
            f"Исходные данные не найдены: {source_path}. "
            "Сначала запустите scripts/download_data.py."
        )

    profile = source_profile(source_path)
    if profile["required_nulls"] != 0 or profile["invalid_event_type"] != 0:
        raise ValueError(f"Проверка исходных данных не пройдена: {profile}")

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
        raise RuntimeError("Количество подготовленных событий не совпало с источником")
    return profile
