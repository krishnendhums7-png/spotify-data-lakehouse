"""Post-run verification: read every output back from disk and run reconciliation checks.

These are *engineering* checks for Member 2's own module (row accounting, types, readability).
Member 4 owns the formal Great Expectations suites.
"""
from __future__ import annotations

import time

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from spark.common import PipelineError, StageResult
from utils.config import PipelineConfig
from utils.logger import get_logger

log = get_logger(__name__)

EXPECTED = {   # layer, dataset -> must-have columns
    ("bronze", "tracks"): ["track_id", "_source_file", "_ingested_at", "_run_id"],
    ("bronze", "charts"): ["url", "streams", "_run_id"],
    ("bronze", "countries"): ["alpha-2", "alpha-3", "_run_id"],
    ("silver", "tracks"): ["track_id", "popularity", "genres", "primary_genre"],
    ("silver", "track_genres"): ["track_id", "genre"],
    ("silver", "charts"): ["chart_date", "chart_rank", "streams", "track_id"],
    ("silver", "countries"): ["iso_alpha2", "iso_alpha3", "region", "sub_region"],
    ("silver", "enriched_chart_data"): ["track_id", "is_track_matched", "region_match_status", "country_iso_alpha3"],
    ("gold", "enriched_chart_data"): ["chart_record_id", "chart_year", "chart_season"],
    ("gold", "music_analytics"): ["chart_record_id", "danceability", "primary_genre"],
    ("gold", "country_analytics"): ["region", "chart_entries"],
    ("gold", "artist_analytics"): ["artist_name", "chart_entries"],
    ("gold", "genre_analytics"): ["genre", "catalogue_tracks"],
    ("gold", "monthly_analytics"): ["chart_year_month", "chart_entries"],
}
EXPECTED_TYPES = {
    ("silver", "charts"): {"chart_date": T.DateType, "chart_rank": T.IntegerType, "streams": T.LongType},
    ("silver", "tracks"): {"popularity": T.IntegerType, "duration_ms": T.LongType, "tempo": T.DoubleType,
                           "explicit": T.BooleanType},
    ("silver", "countries"): {"iso_numeric_code": T.IntegerType},
}


def run_verify(spark: SparkSession, config: PipelineConfig) -> StageResult:
    t0 = time.time()
    log.info("=== VERIFY: read-back and reconciliation ===")
    res = StageResult("verify")
    checks: list[dict] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"check": name, "passed": bool(ok), "detail": detail})
        (log.info if ok else log.error)("[verify] %s %s %s", "PASS" if ok else "FAIL", name, detail)

    counts: dict[tuple, int] = {}
    for (layer, ds), cols in EXPECTED.items():
        path = config.data_dir / layer / ds
        try:
            df = spark.read.parquet(str(path))
            counts[(layer, ds)] = df.count()
            missing = [c for c in cols if c not in df.columns]
            check(f"readable+columns {layer}/{ds}", counts[(layer, ds)] > 0 and not missing,
                  f"rows={counts[(layer, ds)]:,} missing_cols={missing}")
            for col, typ in EXPECTED_TYPES.get((layer, ds), {}).items():
                actual = df.schema[col].dataType
                check(f"type {layer}/{ds}.{col}", isinstance(actual, typ), f"{actual.simpleString()}")
        except Exception as exc:
            check(f"readable {layer}/{ds}", False, str(exc)[:200])

    def quarantine_rows(ds: str) -> int:
        return spark.read.parquet(str(config.quarantine_dir / ds)).count()

    try:
        bc = counts[("bronze", "charts")]; sc = counts[("silver", "charts")]
        rej = quarantine_rows("charts")
        dup = (bc - rej) - sc
        check("charts: bronze = silver + quarantine + duplicates", bc == sc + rej + dup, f"{bc} = {sc} + {rej} + {dup}")
        check("charts: no chart row lost silver -> enriched -> gold",
              sc == counts[("silver", "enriched_chart_data")] == counts[("gold", "enriched_chart_data")]
              == counts[("gold", "music_analytics")], f"{sc} / {counts[('silver','enriched_chart_data')]} / "
              f"{counts[('gold','enriched_chart_data')]} / {counts[('gold','music_analytics')]}")
        bt = counts[("bronze", "tracks")]; st = counts[("silver", "tracks")]
        check("tracks: unique track_id", spark.read.parquet(str(config.silver_path("tracks"))).select("track_id").distinct().count() == st)
        check("tracks: silver <= bronze", st <= bt, f"{st} <= {bt}")
        enr = spark.read.parquet(str(config.gold_path("enriched_chart_data")))
        check("gold: chart_record_id unique", enr.select("chart_record_id").distinct().count() == counts[("gold", "enriched_chart_data")])
        cc = spark.read.parquet(str(config.silver_path("countries")))
        check("countries: every country has an ISO alpha-2 code (Namibia 'NA' recovered from iso_3166_2)",
              cc.filter("iso_alpha2 IS NULL").count() == 0 and
              cc.filter("country_name = 'Namibia' AND iso_alpha2 = 'NA' AND iso_alpha2_recovered").count() == 1)
    except KeyError as exc:
        check("reconciliation inputs available", False, f"missing {exc}")

    res.details["checks"] = checks
    res.seconds = time.time() - t0
    failed = [c for c in checks if not c["passed"]]
    if failed:
        raise PipelineError(f"{len(failed)} verification check(s) failed: {[c['check'] for c in failed]}")
    log.info("[verify] all %d checks passed", len(checks))
    return res
