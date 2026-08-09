from pathlib import Path
import argparse
import json

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MART_DIR = PROJECT_ROOT / "data" / "processed"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "yambda_datalens"


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise FileNotFoundError(
            f"Не найдена витрина {path}. Сначала запустите scripts/build_marts.py"
        )
    return path


def save_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        float_format="%.8f",
    )


def export_product(mart_dir: Path, output_dir: Path) -> pd.DataFrame:
    path = require_file(mart_dir / "mart_product_day.parquet")
    product = pd.read_parquet(path).sort_values("day_idx").reset_index(drop=True)
    product["day_label"] = product["day_idx"].map(
        lambda value: f"День {int(value):03d}"
    )
    edge_days = {product["day_idx"].min(), product["day_idx"].max()}
    product["is_edge_day"] = product["day_idx"].isin(edge_days)
    save_csv(product, output_dir / "01_product" / "product_day.csv")
    return product


def export_tracks(
    mart_dir: Path,
    output_dir: Path,
) -> tuple[pd.DataFrame, dict[str, float]]:
    path = require_file(mart_dir / "mart_track.parquet")
    tracks = pd.read_parquet(
        path,
        filters=[("is_reliable_sample", "=", True)],
    ).sort_values("item_id").reset_index(drop=True)

    listen_plus_q25 = float(tracks["listen_plus_rate"].quantile(0.25))
    listen_plus_q75 = float(tracks["listen_plus_rate"].quantile(0.75))
    dislikes_q75 = float(
        tracks["dislikes_per_1000_listens"].quantile(0.75)
    )

    tracks["track_length_minutes"] = tracks["track_length_seconds"] / 60.0
    tracks["track_length_bucket"] = pd.cut(
        tracks["track_length_seconds"],
        bins=[float("-inf"), 120, 180, 240, 300, float("inf")],
        labels=["до 2 мин", "2–3 мин", "3–4 мин", "4–5 мин", "5+ мин"],
        right=False,
    ).astype("string")
    tracks["track_length_order"] = tracks["track_length_bucket"].map(
        {
            "до 2 мин": 1,
            "2–3 мин": 2,
            "3–4 мин": 3,
            "4–5 мин": 4,
            "5+ мин": 5,
        }
    )

    share = tracks["recommendation_share"]
    tracks["recommendation_share_bucket"] = "100%"
    tracks.loc[share < 1, "recommendation_share_bucket"] = "75–99%"
    tracks.loc[share <= 0.75, "recommendation_share_bucket"] = "50–75%"
    tracks.loc[share <= 0.50, "recommendation_share_bucket"] = "25–50%"
    tracks.loc[
        (share > 0) & (share <= 0.25),
        "recommendation_share_bucket",
    ] = "1–25%"
    tracks.loc[share == 0, "recommendation_share_bucket"] = "0%"
    tracks["recommendation_share_order"] = tracks[
        "recommendation_share_bucket"
    ].map(
        {
            "0%": 1,
            "1–25%": 2,
            "25–50%": 3,
            "50–75%": 4,
            "75–99%": 5,
            "100%": 6,
        }
    )

    high_quality = (
        (tracks["listen_plus_rate"] >= listen_plus_q75)
        & (tracks["dislikes_per_1000_listens"] < dislikes_q75)
    )
    needs_attention = (
        (tracks["listen_plus_rate"] <= listen_plus_q25)
        | (tracks["dislikes_per_1000_listens"] >= dislikes_q75)
    )
    tracks["quality_segment"] = "стабильные"
    tracks.loc[high_quality, "quality_segment"] = "высокое качество"
    tracks.loc[needs_attention, "quality_segment"] = "требует внимания"
    tracks["quality_segment_order"] = tracks["quality_segment"].map(
        {
            "высокое качество": 1,
            "стабильные": 2,
            "требует внимания": 3,
        }
    )
    tracks["recommendation_listen_plus_gap_pp"] = (
        tracks["recommendation_listen_plus_rate"]
        - tracks["organic_listen_plus_rate"]
    ) * 100.0
    tracks["activity_span_days"] = (
        tracks["last_day"] - tracks["first_day"] + 1
    )

    save_csv(tracks, output_dir / "02_tracks" / "track_quality.csv")
    thresholds = {
        "listen_plus_q25": listen_plus_q25,
        "listen_plus_q75": listen_plus_q75,
        "dislikes_per_1000_q75": dislikes_q75,
    }
    return tracks, thresholds


def export_retention(mart_dir: Path, output_dir: Path) -> pd.DataFrame:
    path = require_file(mart_dir / "mart_retention_cohort.parquet")
    retention = pd.read_parquet(path).sort_values(
        [
            "cohort_day",
            "lifetime_day",
            "recommendation_bucket",
            "engagement_bucket",
        ]
    ).reset_index(drop=True)

    retention["cohort_week_start"] = retention["cohort_day"] // 7 * 7
    retention["cohort_week_label"] = retention["cohort_week_start"].map(
        lambda value: f"Дни {int(value):03d}–{int(value) + 6:03d}"
    )
    retention["recommendation_bucket_order"] = retention[
        "recommendation_bucket"
    ].map(
        {
            "0%": 1,
            "1-25%": 2,
            "25-50%": 3,
            "50-75%": 4,
            "75-99%": 5,
            "100%": 6,
        }
    )
    retention["engagement_bucket_order"] = retention[
        "engagement_bucket"
    ].map({"1-10": 1, "11-30": 2, "31-60": 3, "61+": 4})
    observation_end = int(
        (retention["cohort_day"] + retention["lifetime_day"]).max()
    )
    retention["is_mature_d7"] = retention["cohort_day"] <= observation_end - 7
    retention["is_mature_d30"] = (
        retention["cohort_day"] <= observation_end - 30
    )

    save_csv(
        retention,
        output_dir / "03_retention" / "retention_cohort.csv",
    )
    return retention


def export_funnel(mart_dir: Path, output_dir: Path) -> pd.DataFrame:
    path = require_file(mart_dir / "mart_recommendation_funnel.parquet")
    funnel = pd.read_parquet(path).sort_values("step_order").reset_index(drop=True)
    step_names = {
        "all_listens": "все прослушивания",
        "recommendation_listens": "прослушивания из рекомендаций",
        "recommendation_listen_plus": "Listen+ из рекомендаций",
        "recommendation_replays": "повторы из рекомендаций",
    }
    funnel.insert(2, "step_name_ru", funnel["step"].map(step_names))
    save_csv(
        funnel,
        output_dir / "03_retention" / "recommendation_funnel.csv",
    )
    return funnel


def export_source_quality(
    product: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    rows = []
    sources = [
        (
            1,
            "recommendations",
            "рекомендации",
            "recommendation_listens",
            "recommendation_listen_plus",
            "recommendation_replays",
        ),
        (
            2,
            "organic",
            "органика",
            "organic_listens",
            "organic_listen_plus",
            "organic_replays",
        ),
    ]
    for order, code, name, listens_field, plus_field, replay_field in sources:
        listens = int(product[listens_field].sum())
        listen_plus = int(product[plus_field].sum())
        replays = int(product[replay_field].sum())
        rows.append(
            {
                "source_order": order,
                "source_code": code,
                "listens": listens,
                "listen_plus": listen_plus,
                "replays": replays,
                "listen_plus_rate": listen_plus / listens,
                "replay_rate": replays / listens,
                "source_ru": name,
            }
        )
    quality = pd.DataFrame(rows)
    save_csv(
        quality,
        output_dir / "03_retention" / "recommendation_quality.csv",
    )
    return quality


def validate_exports(
    product: pd.DataFrame,
    tracks: pd.DataFrame,
    retention: pd.DataFrame,
    funnel: pd.DataFrame,
    quality: pd.DataFrame,
    output_dir: Path,
) -> None:
    if product.empty or not product["day_idx"].is_unique:
        raise ValueError("Нарушен ключ продуктовой выгрузки")
    if tracks.empty or not tracks["item_id"].is_unique:
        raise ValueError("Нарушен ключ выгрузки треков")
    if int(tracks["listens"].min()) < 100:
        raise ValueError("В выгрузку попали треки с ненадёжной выборкой")
    if tracks["quality_segment"].isna().any():
        raise ValueError("Не для всех треков рассчитан сегмент качества")

    retention_key = [
        "cohort_day",
        "lifetime_day",
        "recommendation_bucket",
        "engagement_bucket",
    ]
    if retention.duplicated(retention_key).any():
        raise ValueError("Нарушен ключ выгрузки ретеншна")
    if not retention["retention_rate"].between(0, 1).all():
        raise ValueError("Ретеншн выходит за диапазон от 0 до 1")
    if funnel["step_order"].tolist() != [1, 2, 3, 4]:
        raise ValueError("Нарушен порядок этапов воронки")
    if len(quality) != 2:
        raise ValueError("В сравнении должны быть рекомендации и органика")

    for path in output_dir.rglob("*.csv"):
        text = path.read_text(encoding="utf-8-sig")
        broken_text = chr(63) * 3
        if broken_text in text or chr(0xFFFD) in text:
            raise ValueError(f"Повреждена кодировка файла {path}")


def export_datalens(mart_dir: Path, output_dir: Path) -> dict[str, object]:
    product = export_product(mart_dir, output_dir)
    tracks, thresholds = export_tracks(mart_dir, output_dir)
    retention = export_retention(mart_dir, output_dir)
    funnel = export_funnel(mart_dir, output_dir)
    quality = export_source_quality(product, output_dir)
    validate_exports(
        product,
        tracks,
        retention,
        funnel,
        quality,
        output_dir,
    )

    files = {}
    for path in sorted(output_dir.rglob("*.csv")):
        files[str(path.relative_to(output_dir))] = {
            "bytes": path.stat().st_size,
        }
    return {
        "rows": {
            "product_day": len(product),
            "track_quality": len(tracks),
            "retention_cohort": len(retention),
            "recommendation_funnel": len(funnel),
            "recommendation_quality": len(quality),
        },
        "track_thresholds": thresholds,
        "files": files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Экспорт витрин Yambda в CSV для DataLens"
    )
    parser.add_argument("--mart-dir", type=Path, default=DEFAULT_MART_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    report = export_datalens(args.mart_dir, args.output_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
