from pathlib import Path

from src.mart_settings import connect, sql_path


def build_user_mart(database_path: Path, output_path: Path) -> dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(database_path, read_only=True)
    connection.execute(
        f"""
        COPY (
            SELECT
                uid,
                min(day_idx) AS first_day,
                max(day_idx) AS last_day,
                count(DISTINCT day_idx) AS active_days,
                count(*) AS events,
                count(*) FILTER (WHERE is_listen) AS listens,
                count(DISTINCT item_id) FILTER (WHERE is_listen) AS unique_tracks,
                count(*) FILTER (WHERE is_listen_plus) AS listen_plus,
                count(*) FILTER (WHERE is_recommendation_listen) AS recommendation_listens,
                count(*) FILTER (WHERE is_replay) AS replays,
                count(*) FILTER (WHERE event_type = 'like') AS likes,
                count(*) FILTER (WHERE event_type = 'dislike') AS dislikes,
                count(*) FILTER (WHERE event_type = 'unlike') AS unlikes,
                count(*) FILTER (WHERE event_type = 'undislike') AS undislikes,
                sum(play_seconds) AS play_seconds,
                count(*) FILTER (WHERE is_session_start) AS sessions,
                count(*) FILTER (WHERE is_listen_plus) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS listen_plus_rate,
                count(*) FILTER (WHERE is_recommendation_listen) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS recommendation_share,
                count(*) FILTER (WHERE is_replay) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0) AS replay_rate,
                sum(play_seconds)
                    / nullif(count(*) FILTER (WHERE is_session_start), 0) / 60.0
                    AS minutes_per_session
            FROM stage_events
            GROUP BY uid
            ORDER BY uid
        ) TO '{sql_path(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows, sessions = connection.execute(
        f"SELECT count(*), sum(sessions) "
        f"FROM read_parquet('{sql_path(output_path)}')"
    ).fetchone()
    connection.close()
    return {"rows": rows, "sessions": sessions}
