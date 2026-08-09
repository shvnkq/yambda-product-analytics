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


def build_user_day_mart(database_path: Path, output_path: Path) -> dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(database_path, read_only=True)
    connection.execute(
        f"""
        COPY (
            SELECT
                uid,
                day_idx,
                count(*) AS events,
                count(*) FILTER (WHERE is_listen) AS listens,
                count(DISTINCT item_id) FILTER (WHERE is_listen) AS unique_tracks,
                count(*) FILTER (WHERE is_recommendation_listen)
                    AS recommendation_listens,
                count(*) FILTER (WHERE is_listen AND is_organic = 1)
                    AS organic_listens,
                count(*) FILTER (WHERE is_listen_plus) AS listen_plus,
                count(*) FILTER (WHERE is_replay) AS replays,
                count(*) FILTER (WHERE event_type = 'like') AS likes,
                count(*) FILTER (WHERE event_type = 'dislike') AS dislikes,
                sum(play_seconds) AS play_seconds,
                count(*) FILTER (WHERE is_session_start) AS sessions,
                count(*) FILTER (WHERE is_recommendation_listen) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS recommendation_share,
                count(*) FILTER (WHERE is_listen_plus) * 1.0
                    / nullif(count(*) FILTER (WHERE is_listen), 0)
                    AS listen_plus_rate,
                sum(play_seconds) / 60.0 AS play_minutes
            FROM stage_events
            GROUP BY uid, day_idx
            ORDER BY uid, day_idx
        ) TO '{sql_path(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows, users = connection.execute(
        f"SELECT count(*), count(DISTINCT uid) "
        f"FROM read_parquet('{sql_path(output_path)}')"
    ).fetchone()
    connection.close()
    return {"rows": rows, "users": users}


def build_retention_mart(database_path: Path, output_path: Path) -> dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(database_path, read_only=True)
    connection.execute(
        f"""
        COPY (
            WITH bounds AS (
                SELECT max(day_idx) AS max_day
                FROM stage_events
            ),
            first_days AS (
                SELECT uid, min(day_idx) AS cohort_day
                FROM stage_events
                WHERE is_listen
                GROUP BY uid
            ),
            first_day_metrics AS (
                SELECT
                    f.uid,
                    f.cohort_day,
                    count(*) FILTER (WHERE e.is_listen) AS first_day_listens,
                    count(*) FILTER (WHERE e.is_session_start) AS first_day_sessions,
                    avg(e.is_recommendation_listen::INT)
                        FILTER (WHERE e.is_listen) AS first_day_recommendation_share,
                    avg(e.is_listen_plus::INT)
                        FILTER (WHERE e.is_listen) AS first_day_listen_plus_rate
                FROM first_days f
                JOIN stage_events e
                  ON e.uid = f.uid AND e.day_idx = f.cohort_day
                GROUP BY f.uid, f.cohort_day
            ),
            cohorts AS (
                SELECT
                    *,
                    CASE
                        WHEN first_day_recommendation_share = 0 THEN '0%'
                        WHEN first_day_recommendation_share <= 0.25 THEN '1-25%'
                        WHEN first_day_recommendation_share <= 0.50 THEN '25-50%'
                        WHEN first_day_recommendation_share <= 0.75 THEN '50-75%'
                        WHEN first_day_recommendation_share < 1 THEN '75-99%'
                        ELSE '100%'
                    END AS recommendation_bucket,
                    CASE
                        WHEN first_day_listens <= 10 THEN '1-10'
                        WHEN first_day_listens <= 30 THEN '11-30'
                        WHEN first_day_listens <= 60 THEN '31-60'
                        ELSE '61+'
                    END AS engagement_bucket
                FROM first_day_metrics
            ),
            cohort_sizes AS (
                SELECT
                    cohort_day,
                    recommendation_bucket,
                    engagement_bucket,
                    count(*) AS cohort_users,
                    avg(first_day_listens) AS avg_first_day_listens,
                    avg(first_day_sessions) AS avg_first_day_sessions,
                    avg(first_day_listen_plus_rate) AS avg_first_day_listen_plus_rate
                FROM cohorts
                GROUP BY cohort_day, recommendation_bucket, engagement_bucket
            ),
            activity AS (
                SELECT DISTINCT uid, day_idx
                FROM stage_events
                WHERE is_listen
            ),
            retained AS (
                SELECT
                    c.cohort_day,
                    c.recommendation_bucket,
                    c.engagement_bucket,
                    a.day_idx - c.cohort_day AS lifetime_day,
                    count(DISTINCT c.uid) AS retained_users
                FROM cohorts c
                JOIN activity a
                  ON a.uid = c.uid AND a.day_idx >= c.cohort_day
                GROUP BY c.cohort_day, c.recommendation_bucket,
                         c.engagement_bucket, lifetime_day
            ),
            cohort_grid AS (
                SELECT
                    c.cohort_day,
                    c.recommendation_bucket,
                    c.engagement_bucket,
                    r.lifetime_day,
                    c.cohort_users,
                    c.avg_first_day_listens,
                    c.avg_first_day_sessions,
                    c.avg_first_day_listen_plus_rate
                FROM cohort_sizes c
                CROSS JOIN bounds b
                CROSS JOIN range(0, b.max_day - c.cohort_day + 1) r(lifetime_day)
            )
            SELECT
                g.cohort_day,
                g.lifetime_day,
                g.recommendation_bucket,
                g.engagement_bucket,
                g.cohort_users,
                coalesce(r.retained_users, 0) AS retained_users,
                coalesce(r.retained_users, 0) * 1.0 / g.cohort_users
                    AS retention_rate,
                g.avg_first_day_listens,
                g.avg_first_day_sessions,
                g.avg_first_day_listen_plus_rate
            FROM cohort_grid g
            LEFT JOIN retained r
              USING (
                  cohort_day,
                  recommendation_bucket,
                  engagement_bucket,
                  lifetime_day
              )
            ORDER BY cohort_day, recommendation_bucket,
                     engagement_bucket, lifetime_day
        ) TO '{sql_path(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows = connection.execute(
        f"SELECT count(*) FROM read_parquet('{sql_path(output_path)}')"
    ).fetchone()[0]
    connection.close()
    return {"rows": rows}
