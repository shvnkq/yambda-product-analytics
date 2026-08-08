"""Participant 2: build the daily product mart."""

from pathlib import Path

from src.mart_settings import connect, sql_path


def build_product_mart(database_path: Path, output_path: Path) -> dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(database_path, read_only=True)
    connection.execute(
        f"""
        COPY (
            SELECT
                day_idx,
                count(DISTINCT uid) AS active_users,
                count(*) AS events,
                count(*) FILTER (WHERE is_listen) AS listens,
                count(DISTINCT uid) FILTER (WHERE is_listen) AS listeners,
                count(DISTINCT item_id) FILTER (WHERE is_listen) AS unique_tracks,
                count(*) FILTER (WHERE is_listen_plus) AS listen_plus,
                count(*) FILTER (WHERE is_recommendation_listen) AS recommendation_listens,
                count(*) FILTER (WHERE is_listen AND is_organic = 1) AS organic_listens,
                count(*) FILTER (WHERE is_replay) AS replays,
                count(*) FILTER (WHERE event_type = 'like') AS likes,
                count(*) FILTER (WHERE event_type = 'dislike') AS dislikes,
                sum(play_seconds) AS play_seconds,
                count(*) FILTER (WHERE is_session_start) AS sessions,
                count(*) FILTER (WHERE is_listen_plus) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS listen_plus_rate,
                count(*) FILTER (WHERE is_replay) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS replay_rate,
                count(*) FILTER (WHERE is_recommendation_listen) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS recommendation_share,
                count(*) FILTER (WHERE event_type = 'like') * 1000.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS likes_per_1000_listens,
                count(*) FILTER (WHERE event_type = 'dislike') * 1000.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS dislikes_per_1000_listens,
                sum(play_seconds) / nullif(count(DISTINCT uid), 0) / 60.0
                    AS minutes_per_active_user
            FROM stage_events
            GROUP BY day_idx
            ORDER BY day_idx
        ) TO '{sql_path(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows, events, listens = connection.execute(
        f"SELECT count(*), sum(events), sum(listens) "
        f"FROM read_parquet('{sql_path(output_path)}')"
    ).fetchone()
    connection.close()
    return {"rows": rows, "events": events, "listens": listens}
