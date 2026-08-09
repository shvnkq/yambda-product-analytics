from pathlib import Path

from src.mart_settings import TRACK_MIN_LISTENS, connect, sql_path


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
                count(*) FILTER (WHERE is_listen AND is_organic = 1) AS organic_listens,
                count(*) FILTER (WHERE is_recommendation_listen AND is_listen_plus)
                    AS recommendation_listen_plus,
                count(*) FILTER (WHERE is_listen AND is_organic = 1 AND is_listen_plus)
                    AS organic_listen_plus,
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
                count(*) FILTER (WHERE is_recommendation_listen AND is_listen_plus) * 1.0
                    / nullif(count(*) FILTER (WHERE is_recommendation_listen), 0)
                    AS recommendation_listen_plus_rate,
                count(*) FILTER (WHERE is_listen AND is_organic = 1 AND is_listen_plus) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen AND is_organic = 1), 0)
                    AS organic_listen_plus_rate,
                count(*) FILTER (WHERE is_replay) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS replay_rate,
                count(*) FILTER (WHERE event_type = 'like') * 1000.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS likes_per_1000_listens,
                count(*) FILTER (WHERE event_type = 'dislike') * 1000.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS dislikes_per_1000_listens,
                count(*) FILTER (WHERE is_listen) >= {TRACK_MIN_LISTENS}
                    AS is_reliable_sample
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
    user_day_path: Path,
    retention_path: Path,
    track_path: Path,
    funnel_path: Path,
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
    user_day_rows, user_day_keys, user_day_listens = connection.execute(
        f"SELECT count(*), count(DISTINCT (uid, day_idx)), sum(listens) "
        f"FROM read_parquet('{sql_path(user_day_path)}')"
    ).fetchone()
    retention_rows, retention_keys, invalid_retention, invalid_d0 = connection.execute(
        f"""
        SELECT
            count(*),
            count(DISTINCT (
                cohort_day,
                lifetime_day,
                recommendation_bucket,
                engagement_bucket
            )),
            count(*) FILTER (WHERE retention_rate NOT BETWEEN 0 AND 1),
            count(*) FILTER (WHERE lifetime_day = 0 AND retention_rate <> 1)
        FROM read_parquet('{sql_path(retention_path)}')
        """
    ).fetchone()
    track_rows, unique_tracks = connection.execute(
        f"SELECT count(*), count(DISTINCT item_id) "
        f"FROM read_parquet('{sql_path(track_path)}')"
    ).fetchone()
    funnel_rows, funnel_start, funnel_monotonic = connection.execute(
        f"""
        WITH funnel AS (
            SELECT *, lag(events) OVER (ORDER BY step_order) AS previous_events
            FROM read_parquet('{sql_path(funnel_path)}')
        )
        SELECT
            count(*),
            max(events) FILTER (WHERE step_order = 1),
            bool_and(previous_events IS NULL OR events <= previous_events)
        FROM funnel
        """
    ).fetchone()
    connection.close()
    checks = {
        "product_events_match_source": product_events == stage_events,
        "product_listens_match_source": product_listens == stage_listens,
        "user_key_is_unique": user_rows == unique_users == stage_users,
        "user_day_key_is_unique": user_day_rows == user_day_keys,
        "user_day_listens_match_source": user_day_listens == stage_listens,
        "retention_key_is_unique": retention_rows == retention_keys,
        "retention_rates_are_valid": invalid_retention == 0 and invalid_d0 == 0,
        "track_key_is_unique": track_rows == unique_tracks == stage_tracks,
        "funnel_is_valid": (
            funnel_rows == 4 and funnel_start == stage_listens and funnel_monotonic
        ),
    }
    if not all(checks.values()):
        raise RuntimeError(f"Сверка витрин не пройдена: {checks}")
    return checks
