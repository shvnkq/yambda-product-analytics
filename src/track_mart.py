from pathlib import Path

from src.mart_settings import connect, sql_path


def build_track_mart(database_path: Path, output_path: Path) -> dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(database_path, read_only=True)
    connection.execute(
        f"""
        COPY (
            SELECT
                item_id,
                min(day_idx) AS first_day,
                max(day_idx) AS last_day,
                count(*) AS events,
                count(*) FILTER (WHERE is_listen) AS listens,
                count(DISTINCT uid) FILTER (WHERE is_listen) AS listeners,
                count(*) FILTER (WHERE is_listen_plus) AS listen_plus,
                count(*) FILTER (WHERE is_recommendation_listen) AS recommendation_listens,
                count(*) FILTER (WHERE is_replay) AS replays,
                count(*) FILTER (WHERE event_type = 'like') AS likes,
                count(*) FILTER (WHERE event_type = 'dislike') AS dislikes,
                avg(played_ratio_pct) FILTER (WHERE is_listen) AS avg_played_ratio,
                any_value(track_length_seconds) FILTER (WHERE is_listen)
                    AS track_length_seconds,
                count(*) FILTER (WHERE is_listen_plus) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS listen_plus_rate,
                count(*) FILTER (WHERE is_recommendation_listen) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS recommendation_share,
                count(*) FILTER (WHERE is_replay) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS replay_rate,
                count(*) FILTER (WHERE event_type = 'like') * 1000.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS likes_per_1000_listens,
                count(*) FILTER (WHERE event_type = 'dislike') * 1000.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS dislikes_per_1000_listens
            FROM stage_events
            GROUP BY item_id
            ORDER BY item_id
        ) TO '{sql_path(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows = connection.execute(
        f"SELECT count(*) FROM read_parquet('{sql_path(output_path)}')"
    ).fetchone()[0]
    connection.close()
    return {"rows": rows}


def validate_all_marts(
    database_path: Path,
    product_path: Path,
    user_path: Path,
    track_path: Path,
) -> dict[str, bool]:
    connection = connect(database_path, read_only=True)
    stage_events, stage_listens, stage_users, stage_tracks = connection.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE is_listen),
               count(DISTINCT uid), count(DISTINCT item_id)
        FROM stage_events
        """
    ).fetchone()
    product_events, product_listens = connection.execute(
        f"SELECT sum(events), sum(listens) "
        f"FROM read_parquet('{sql_path(product_path)}')"
    ).fetchone()
    user_rows, unique_users = connection.execute(
        f"SELECT count(*), count(DISTINCT uid) "
        f"FROM read_parquet('{sql_path(user_path)}')"
    ).fetchone()
    track_rows, unique_tracks = connection.execute(
        f"SELECT count(*), count(DISTINCT item_id) "
        f"FROM read_parquet('{sql_path(track_path)}')"
    ).fetchone()
    connection.close()
    checks = {
        "product_events_match_source": product_events == stage_events,
        "product_listens_match_source": product_listens == stage_listens,
        "user_key_is_unique": user_rows == unique_users == stage_users,
        "track_key_is_unique": track_rows == unique_tracks == stage_tracks,
    }
    if not all(checks.values()):
        raise RuntimeError(f"Сверка витрин не пройдена: {checks}")
    return checks
