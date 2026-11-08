"""Great Expectations validation execution engine for Spotify Lakehouse (Data Quality).

Uses the official Great Expectations API (gx 1.24.1 / gxe) to execute expectation suites.
- Validates Bronze, Silver, Gold, and Marts layers.
- Guarantees that unsupported expectation types NEVER silently pass.
- Missing datasets produce explicit FAILED status, never silent success.
- Cross-table checks (referential integrity & quarantine budget) are clearly
  separated as Custom Integration Checks and explicitly reported.
- Generates genuine validation reports in data_quality/reports/.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Official Great Expectations Imports
try:
    import great_expectations as gx
    import great_expectations.expectations as gxe
    HAVE_GX = True
except ImportError:
    HAVE_GX = False

try:
    from data_quality.quality.expectations_definitions import (
        ALL_SUITES,
        SUPPORTED_EXPECTATION_TYPES,
    )
except ImportError:
    try:
        from member4.quality.expectations_definitions import (
            ALL_SUITES,
            SUPPORTED_EXPECTATION_TYPES,
        )
    except ImportError:
        from quality.expectations_definitions import (
            ALL_SUITES,
            SUPPORTED_EXPECTATION_TYPES,
        )

logger = logging.getLogger("data_quality_validator")
logger.setLevel(logging.INFO)


def get_project_root() -> Path:
    """Resolve the absolute root directory of the Spotify Lakehouse repository."""
    env_root = os.getenv("SPOTIFY_PROJECT_ROOT")
    if env_root and Path(env_root).exists():
        return Path(env_root).resolve()

    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "dbt_spotify").exists() and ((parent / "data_quality").exists() or (parent / "member4").exists()):
            return parent
        if (parent / "ingestion").exists() and (parent / "spark").exists():
            return parent
        if (parent / "data_quality").exists() and (parent / "data").exists():
            return parent
    return current.parents[2]


PROJECT_ROOT = get_project_root()
DATA_QUALITY_DIR = (
    PROJECT_ROOT / "data_quality"
    if (PROJECT_ROOT / "data_quality").exists()
    else Path(__file__).resolve().parents[1]
)
REPORTS_DIR = DATA_QUALITY_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


class UnsupportedExpectationError(Exception):
    """Raised when an expectation type is not supported and strict mode is active."""
    pass


@dataclass
class ExpectationEvaluation:
    expectation_type: str
    dimension: str
    description: str
    success: bool
    observed_value: Any
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    element_count: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "expectation_config": {
                "expectation_type": self.expectation_type,
                "meta": {
                    "dimension": self.dimension,
                    "description": self.description,
                },
            },
            "result": {
                "element_count": self.element_count,
                "unexpected_count": self.unexpected_count,
                "unexpected_percent": self.unexpected_percent,
                "observed_value": self.observed_value,
                "details": self.details,
            },
        }


@dataclass
class SuiteValidationResult:
    suite_name: str
    target_asset: str
    layer: str
    success: bool
    total_expectations: int
    passed_expectations: int
    failed_expectations: int
    evaluations: list[ExpectationEvaluation]
    engine: str = "GreatExpectations_1.24"  # or "CustomIntegrationValidator"
    status: str = "PASSED"  # PASSED, FAILED, MISSING_DATASET, ERROR
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "target_asset": self.target_asset,
            "layer": self.layer,
            "success": self.success,
            "status": self.status,
            "engine": self.engine,
            "statistics": {
                "total_expectations": self.total_expectations,
                "passed_expectations": self.passed_expectations,
                "failed_expectations": self.failed_expectations,
                "success_percent": round(
                    (self.passed_expectations / self.total_expectations * 100)
                    if self.total_expectations > 0 else 0,
                    2,
                ),
            },
            "evaluations": [e.to_dict() for e in self.evaluations],
            "timestamp": self.timestamp,
        }


def _build_gxe_expectation(exp_type: str, kwargs: dict[str, Any]) -> Any:
    """Map declarative expectation definition to native Great Expectations 1.24.1 gxe class."""
    if exp_type == "expect_table_columns_to_match_set":
        return gxe.ExpectTableColumnsToMatchSet(
            column_set=kwargs["column_set"],
            exact_match=kwargs.get("exact_match", False),
        )
    if exp_type == "expect_table_row_count_to_be_between":
        return gxe.ExpectTableRowCountToBeBetween(
            min_value=kwargs.get("min_value", 0),
            max_value=kwargs.get("max_value"),
        )
    if exp_type == "expect_column_values_to_not_be_null":
        return gxe.ExpectColumnValuesToNotBeNull(column=kwargs["column"])
    if exp_type == "expect_column_values_to_be_unique":
        return gxe.ExpectColumnValuesToBeUnique(column=kwargs["column"])
    if exp_type == "expect_compound_columns_to_be_unique":
        cols = kwargs.get("column_list") or kwargs.get("columns", [])
        return gxe.ExpectCompoundColumnsToBeUnique(column_list=cols)
    if exp_type == "expect_column_values_to_be_between":
        return gxe.ExpectColumnValuesToBeBetween(
            column=kwargs["column"],
            min_value=kwargs.get("min_value"),
            max_value=kwargs.get("max_value"),
        )
    if exp_type == "expect_column_values_to_be_in_set":
        return gxe.ExpectColumnValuesToBeInSet(
            column=kwargs["column"],
            value_set=kwargs["value_set"],
        )
    if exp_type == "expect_column_values_to_match_regex":
        return gxe.ExpectColumnValuesToMatchRegex(
            column=kwargs["column"],
            regex=kwargs["regex"],
        )
    if exp_type == "expect_column_value_lengths_to_equal":
        return gxe.ExpectColumnValueLengthsToEqual(
            column=kwargs["column"],
            value=kwargs["value"],
        )
    return None


class GreatExpectationsValidator:
    """Executes Great Expectations suites on lakehouse datasets using official GX APIs."""

    def __init__(self, data_dir: Path | str | None = None, strict_unsupported: bool = True):
        self.data_dir = Path(data_dir) if data_dir else PROJECT_ROOT / "data"
        self.exports_dir = PROJECT_ROOT / "dbt_spotify" / "exports"
        self.reports_dir = REPORTS_DIR
        self.strict_unsupported = strict_unsupported

    def _read_dataset(self, layer: str, asset: str) -> pd.DataFrame | None:
        """Read Parquet dataset from lakehouse or dbt exports."""
        # 1. Lakehouse paths. Member 2 stores rejected Silver tracks under
        # data/silver/_quarantine/tracks, while the suite calls this asset
        # "tracks_quarantine". Keep the logical suite name and resolve its
        # physical location here.
        if layer == "silver" and asset == "tracks_quarantine":
            candidates = [
                self.data_dir / "silver" / "_quarantine" / "tracks",
                self.data_dir / "silver" / "_quarantine" / "tracks.parquet",
            ]
        else:
            candidates = [
                self.data_dir / layer / asset,
                self.data_dir / layer / f"{asset}.parquet",
                self.data_dir / layer / asset / "data.parquet",
            ]
        for p in candidates:
            if p.exists():
                try:
                    return pd.read_parquet(p)
                except Exception as e:
                    logger.debug(f"Could not read parquet from {p}: {e}")

        # 2. dbt exports path: dbt_spotify/exports/{core|analytics}/{asset}.parquet
        for sub in ("core", "analytics"):
            p = self.exports_dir / sub / f"{asset}.parquet"
            if p.exists():
                try:
                    return pd.read_parquet(p)
                except Exception as e:
                    logger.debug(f"Could not read parquet from {p}: {e}")

        return None

    def _execute_native_gx_suite(
        self,
        df: pd.DataFrame,
        suite_name: str,
        expectations: list[dict[str, Any]],
    ) -> list[ExpectationEvaluation]:
        """Execute a suite natively via official Great Expectations 1.24.1 API."""
        if not HAVE_GX:
            raise RuntimeError("Great Expectations is not installed in this environment.")

        # Strict validation: verify all expectation types upfront
        for exp in expectations:
            etype = exp.get("expectation_type", "")
            if etype not in SUPPORTED_EXPECTATION_TYPES:
                err_msg = f"CRITICAL: Unsupported expectation type '{etype}'. Silent passing is prohibited."
                logger.error(err_msg)
                if self.strict_unsupported:
                    raise UnsupportedExpectationError(err_msg)
                return [
                    ExpectationEvaluation(
                        expectation_type=etype,
                        dimension="unsupported",
                        description=f"Unsupported expectation: {etype}",
                        success=False,
                        observed_value="UNSUPPORTED_TYPE",
                        unexpected_count=1,
                        element_count=len(df),
                        details={"error": err_msg},
                    )
                ]

        context = gx.get_context(mode="ephemeral")
        ds_name = f"ds_{suite_name}_{int(time.time() * 1000)}"
        datasource = context.data_sources.add_pandas(ds_name)
        data_asset = datasource.add_dataframe_asset(name="asset")
        batch_def = data_asset.add_batch_definition_whole_dataframe("batch")

        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        gxe_map: list[tuple[dict[str, Any], Any]] = []

        for exp in expectations:
            etype = exp.get("expectation_type", "")
            kwargs = exp.get("kwargs", {})
            gxe_obj = _build_gxe_expectation(etype, kwargs)
            if gxe_obj is not None:
                suite.add_expectation(gxe_obj)
                gxe_map.append((exp, gxe_obj))

        val_def = context.validation_definitions.add(
            gx.ValidationDefinition(name=f"val_{suite_name}", data=batch_def, suite=suite)
        )
        gx_result = val_def.run(batch_parameters={"dataframe": df})

        # GX result ordering is not guaranteed to match the order in which
        # expectations were added. Match each result to its expectation config
        # using its type and authored kwargs; never zip by position.
        remaining = list(gxe_map)
        evaluations: list[ExpectationEvaluation] = []

        def result_config_parts(result: Any) -> tuple[str, dict[str, Any]]:
            config = getattr(result, "expectation_config", None)
            if config is None:
                return "", {}
            config_type = (
                getattr(config, "type", None)
                or getattr(config, "expectation_type", None)
                or ""
            )
            config_kwargs = getattr(config, "kwargs", {}) or {}
            return str(config_type), dict(config_kwargs)

        def find_matching_definition(result: Any) -> int | None:
            result_type, result_kwargs = result_config_parts(result)
            candidates: list[int] = []
            for index, (definition, _) in enumerate(remaining):
                if definition.get("expectation_type", "") != result_type:
                    continue
                authored_kwargs = definition.get("kwargs", {}) or {}
                # GX may add internal/default kwargs. Only require the kwargs
                # explicitly supplied in our declarative definition to match.
                if all(
                    result_kwargs.get(key) == value
                    for key, value in authored_kwargs.items()
                    if key != "allow_null"
                ):
                    candidates.append(index)
            return candidates[0] if len(candidates) == 1 else None

        for res in gx_result.results:
            match_index = find_matching_definition(res)
            if match_index is None:
                result_type, result_kwargs = result_config_parts(res)
                evaluations.append(
                    ExpectationEvaluation(
                        expectation_type=result_type or "unmatched_gx_result",
                        dimension="data_quality",
                        description="GX returned a result that could not be uniquely matched to its configured expectation",
                        success=False,
                        observed_value="UNMATCHED_OR_AMBIGUOUS_GX_RESULT",
                        unexpected_count=1,
                        unexpected_percent=100.0,
                        element_count=len(df),
                        details={
                            "gx_native": True,
                            "result_kwargs": result_kwargs,
                            "error": "Expectation result mapping was ambiguous or unmatched.",
                        },
                    )
                )
                continue

            exp_def, _ = remaining.pop(match_index)
            meta = exp_def.get("meta", {})
            dim = meta.get("dimension", "data_quality")
            desc = meta.get("description", exp_def.get("expectation_type", ""))
            res_dict = res.result or {}
            success = bool(res.success)
            unexpected_count = res_dict.get("unexpected_count", 0 if success else 1)
            unexpected_pct = res_dict.get("unexpected_percent", 0.0 if success else 100.0)
            element_count = res_dict.get("element_count", len(df))

            observed = res_dict.get("observed_value")
            if observed is None:
                observed = "PASS (GX 1.24)" if success else "FAIL (GX 1.24)"

            evaluations.append(
                ExpectationEvaluation(
                    expectation_type=exp_def.get("expectation_type", ""),
                    dimension=dim,
                    description=desc,
                    success=success,
                    observed_value=observed,
                    unexpected_count=unexpected_count,
                    unexpected_percent=unexpected_pct,
                    element_count=element_count,
                    details={"gx_native": True, "result": res_dict},
                )
            )

        # If GX omits a result, record an explicit failure for that expectation.
        for exp_def, _ in remaining:
            meta = exp_def.get("meta", {})
            evaluations.append(
                ExpectationEvaluation(
                    expectation_type=exp_def.get("expectation_type", ""),
                    dimension=meta.get("dimension", "data_quality"),
                    description=meta.get("description", exp_def.get("expectation_type", "")),
                    success=False,
                    observed_value="MISSING_GX_RESULT",
                    unexpected_count=1,
                    unexpected_percent=100.0,
                    element_count=len(df),
                    details={"gx_native": True, "error": "GX did not return a result for this expectation."},
                )
            )

        return evaluations

    def _execute_custom_integration_suite(
        self,
        suite_name: str,
        df: pd.DataFrame,
        expectations: list[dict[str, Any]],
        context_dfs: dict[str, pd.DataFrame],
    ) -> list[ExpectationEvaluation]:
        """Execute cross-table and quarantine checks as explicitly separated integration checks."""
        evaluations: list[ExpectationEvaluation] = []
        for exp in expectations:
            etype = exp.get("expectation_type", "")
            kwargs = exp.get("kwargs", {})
            meta = exp.get("meta", {})
            dim = meta.get("dimension", "integration")
            desc = meta.get("description", etype)

            if etype not in SUPPORTED_EXPECTATION_TYPES:
                if self.strict_unsupported:
                    raise UnsupportedExpectationError(f"Unsupported expectation: {etype}")
                evaluations.append(
                    ExpectationEvaluation(
                        expectation_type=etype,
                        dimension=dim,
                        description=desc,
                        success=False,
                        observed_value="UNSUPPORTED_TYPE",
                        unexpected_count=1,
                        element_count=len(df),
                    )
                )
                continue

            if etype == "expect_foreign_keys_to_exist":
                child_ds = kwargs.get("child_dataset", "")
                child_col = kwargs.get("child_column", "")
                parent_ds = kwargs.get("parent_dataset", "")
                parent_col = kwargs.get("parent_column", "")

                child_df = context_dfs.get(child_ds) if child_ds in context_dfs else df
                parent_df = context_dfs.get(parent_ds)

                if child_df is None or parent_df is None:
                    missing = parent_ds if parent_df is None else child_ds
                    evaluations.append(
                        ExpectationEvaluation(
                            expectation_type=etype,
                            dimension=dim,
                            description=desc,
                            success=False,
                            observed_value=f"MISSING_CONTEXT_TABLE: {missing}",
                            unexpected_count=1,
                            element_count=len(df),
                            details={"custom_integration_check": True, "error": f"Dataset '{missing}' unavailable."},
                        )
                    )
                    continue

                if child_col not in child_df.columns or parent_col not in parent_df.columns:
                    missing_col = child_col if child_col not in child_df.columns else parent_col
                    evaluations.append(
                        ExpectationEvaluation(
                            expectation_type=etype,
                            dimension=dim,
                            description=desc,
                            success=False,
                            observed_value=f"MISSING_COLUMN: {missing_col}",
                            unexpected_count=1,
                            element_count=len(df),
                            details={"custom_integration_check": True},
                        )
                    )
                    continue

                child_keys = child_df[child_col].dropna()
                parent_keys = set(parent_df[parent_col].dropna())
                orphans = child_keys[~child_keys.isin(parent_keys)]
                orphan_count = len(orphans)
                success = orphan_count == 0

                evaluations.append(
                    ExpectationEvaluation(
                        expectation_type=etype,
                        dimension=dim,
                        description=desc,
                        success=success,
                        observed_value=f"{orphan_count} orphan keys" if not success else "All FKs resolved",
                        unexpected_count=orphan_count,
                        unexpected_percent=round(orphan_count / len(child_keys) * 100, 2) if len(child_keys) > 0 else 0.0,
                        element_count=len(child_df),
                        details={"custom_integration_check": True, "orphan_sample": orphans.head(5).tolist()},
                    )
                )

            elif etype == "expect_bridge_weights_to_sum_to_one":
                ds_name = kwargs.get("dataset", "")
                group_col = kwargs.get("group_column", "")
                weight_col = kwargs.get("weight_column", "")

                bridge_df = context_dfs.get(ds_name) if ds_name in context_dfs else df
                if bridge_df is None or group_col not in bridge_df.columns or weight_col not in bridge_df.columns:
                    evaluations.append(
                        ExpectationEvaluation(
                            expectation_type=etype,
                            dimension=dim,
                            description=desc,
                            success=False,
                            observed_value="TABLE_OR_COL_MISSING",
                            unexpected_count=1,
                            element_count=len(df),
                            details={"custom_integration_check": True},
                        )
                    )
                    continue

                sums = bridge_df.groupby(group_col)[weight_col].sum()
                invalid_weights = sums[(sums - 1.0).abs() > 1e-4]
                invalid_count = len(invalid_weights)
                success = invalid_count == 0

                evaluations.append(
                    ExpectationEvaluation(
                        expectation_type=etype,
                        dimension=dim,
                        description=desc,
                        success=success,
                        observed_value=f"{invalid_count} groups with sum != 1.0" if not success else "All weight sums == 1.0",
                        unexpected_count=invalid_count,
                        unexpected_percent=round(invalid_count / len(sums) * 100, 2) if len(sums) > 0 else 0.0,
                        element_count=len(sums),
                        details={"custom_integration_check": True, "invalid_sample": invalid_weights.head(5).to_dict()},
                    )
                )

            elif etype == "expect_quarantine_records_to_have_reasons":
                reason_col = kwargs.get("reason_column", "reject_reasons")
                if reason_col not in df.columns:
                    evaluations.append(
                        ExpectationEvaluation(
                            expectation_type=etype,
                            dimension=dim,
                            description=desc,
                            success=False,
                            observed_value=f"COLUMN_MISSING: {reason_col}",
                            unexpected_count=len(df),
                            element_count=len(df),
                            details={"custom_integration_check": True},
                        )
                    )
                    continue

                missing_reasons = df[df[reason_col].isna() | (df[reason_col].astype(str).str.strip() == "")]
                unexp_count = len(missing_reasons)
                success = unexp_count == 0

                evaluations.append(
                    ExpectationEvaluation(
                        expectation_type=etype,
                        dimension=dim,
                        description=desc,
                        success=success,
                        observed_value=f"{unexp_count} records missing reasons" if not success else "All reasons present",
                        unexpected_count=unexp_count,
                        unexpected_percent=round(unexp_count / len(df) * 100, 2) if len(df) > 0 else 0.0,
                        element_count=len(df),
                        details={"custom_integration_check": True},
                    )
                )

            elif etype == "expect_quarantine_error_rate_to_be_below_threshold":
                total_ds_name = kwargs.get("total_dataset", "silver_tracks")
                threshold_pct = float(kwargs.get("threshold_pct", 0.1))
                total_df = context_dfs.get(total_ds_name)

                quarantine_count = len(df)
                clean_count = len(total_df) if total_df is not None else 0
                total_records = clean_count + quarantine_count

                rate = round((quarantine_count / total_records) * 100, 3) if total_records > 0 else 0.0
                success = rate <= threshold_pct
                evaluations.append(
                    ExpectationEvaluation(
                        expectation_type=etype,
                        dimension=dim,
                        description=desc,
                        success=success,
                        observed_value=f"{rate}% (threshold: {threshold_pct}%)",
                        unexpected_count=quarantine_count if not success else 0,
                        unexpected_percent=rate,
                        element_count=total_records,
                        details={"custom_integration_check": True, "quarantine_count": quarantine_count, "clean_count": clean_count},
                    )
                )

        return evaluations

    def evaluate_expectation(
        self,
        df: pd.DataFrame,
        expectation: dict[str, Any],
        context_dfs: dict[str, pd.DataFrame] | None = None,
    ) -> ExpectationEvaluation:
        """Validate a single expectation for isolated evaluation and testing."""
        etype = expectation.get("expectation_type", "")
        meta = expectation.get("meta", {})
        dim = meta.get("dimension", "data_quality")
        desc = meta.get("description", etype)

        if etype not in SUPPORTED_EXPECTATION_TYPES:
            err_msg = f"Unsupported expectation type '{etype}'. Silent passing is prohibited."
            logger.error(err_msg)
            if self.strict_unsupported:
                raise UnsupportedExpectationError(err_msg)
            return ExpectationEvaluation(
                expectation_type=etype,
                dimension=dim,
                description=desc,
                success=False,
                observed_value="UNSUPPORTED_EXPECTATION_TYPE",
                unexpected_count=1,
                element_count=len(df),
                details={"error": err_msg},
            )

        if etype in (
            "expect_foreign_keys_to_exist",
            "expect_bridge_weights_to_sum_to_one",
            "expect_quarantine_records_to_have_reasons",
            "expect_quarantine_error_rate_to_be_below_threshold",
        ):
            results = self._execute_custom_integration_suite(
                "single_eval", df, [expectation], context_dfs or {}
            )
            return results[0]

        results = self._execute_native_gx_suite(df, "single_eval", [expectation])
        return results[0]

    def validate_suite(
        self,
        suite_name: str,
        df: pd.DataFrame | None,
        layer: str,
        asset_name: str,
        context_dfs: dict[str, pd.DataFrame] | None = None,
    ) -> SuiteValidationResult:
        """Validate a single expectation suite. If df is None, records explicit failure."""
        expectations = ALL_SUITES.get(suite_name, [])
        context = dict(context_dfs or {})

        # MISSING DATASET HANDLING: Must fail, never silently pass!
        if df is None:
            err_eval = ExpectationEvaluation(
                expectation_type="expect_dataset_to_exist",
                dimension="availability",
                description=f"Dataset '{asset_name}' must exist on disk in layer '{layer}'",
                success=False,
                observed_value="DATASET_NOT_FOUND",
                unexpected_count=1,
                unexpected_percent=100.0,
                element_count=0,
                details={"error": f"Required dataset '{asset_name}' not found on disk in layer '{layer}'."},
            )
            return SuiteValidationResult(
                suite_name=suite_name,
                target_asset=asset_name,
                layer=layer,
                success=False,
                status="MISSING_DATASET",
                engine="GreatExpectations_1.24",
                total_expectations=len(expectations) or 1,
                passed_expectations=0,
                failed_expectations=len(expectations) or 1,
                evaluations=[err_eval],
            )

        is_custom_integration = suite_name in ("referential_integrity_suite", "quarantine_isolation_suite")

        if is_custom_integration:
            evaluations = self._execute_custom_integration_suite(suite_name, df, expectations, context)
            engine = "CustomIntegrationValidator"
        else:
            evaluations = self._execute_native_gx_suite(df, suite_name, expectations)
            engine = "GreatExpectations_1.24"

        total = len(evaluations)
        passed = sum(1 for e in evaluations if e.success)
        failed = total - passed
        success = failed == 0

        return SuiteValidationResult(
            suite_name=suite_name,
            target_asset=asset_name,
            layer=layer,
            success=success,
            status="PASSED" if success else "FAILED",
            engine=engine,
            total_expectations=total,
            passed_expectations=passed,
            failed_expectations=failed,
            evaluations=evaluations,
        )

    def run_layer_validation(
        self,
        layer: str,
        context_dfs: dict[str, pd.DataFrame] | None = None,
    ) -> list[SuiteValidationResult]:
        """Run all suites relevant to a lakehouse layer. Guarantees all suites are accounted for."""
        results: list[SuiteValidationResult] = []
        context = dict(context_dfs or {})

        # Bronze Layer Execution (3 suites)
        if layer in ("bronze", "all"):
            raw_tracks = context.get("bronze_tracks") if "bronze_tracks" in context else self._read_dataset("bronze", "tracks")
            raw_charts = context.get("bronze_charts") if "bronze_charts" in context else self._read_dataset("bronze", "charts")
            raw_countries = context.get("bronze_countries") if "bronze_countries" in context else self._read_dataset("bronze", "countries")

            results.append(self.validate_suite("bronze_tracks_suite", raw_tracks, "bronze", "tracks", context))
            results.append(self.validate_suite("bronze_charts_suite", raw_charts, "bronze", "charts", context))
            results.append(self.validate_suite("bronze_countries_suite", raw_countries, "bronze", "countries", context))

        # Silver Layer Execution (5 suites)
        if layer in ("silver", "all"):
            tracks_df = context.get("silver_tracks") if "silver_tracks" in context else self._read_dataset("silver", "tracks")
            charts_df = context.get("silver_charts") if "silver_charts" in context else self._read_dataset("silver", "charts")
            countries_df = context.get("silver_countries") if "silver_countries" in context else self._read_dataset("silver", "countries")
            genres_df = context.get("silver_track_genres") if "silver_track_genres" in context else self._read_dataset("silver", "track_genres")
            quarantine_df = context.get("tracks_quarantine") if "tracks_quarantine" in context else self._read_dataset("silver", "tracks_quarantine")

            if tracks_df is not None: context["silver_tracks"] = tracks_df
            if charts_df is not None: context["silver_charts"] = charts_df
            if countries_df is not None: context["silver_countries"] = countries_df
            if genres_df is not None: context["silver_track_genres"] = genres_df
            if quarantine_df is not None: context["tracks_quarantine"] = quarantine_df

            results.append(self.validate_suite("silver_tracks_suite", tracks_df, "silver", "tracks", context))
            results.append(self.validate_suite("silver_charts_suite", charts_df, "silver", "charts", context))
            results.append(self.validate_suite("silver_countries_suite", countries_df, "silver", "countries", context))
            results.append(self.validate_suite("silver_track_genres_suite", genres_df, "silver", "track_genres", context))
            results.append(self.validate_suite("quarantine_isolation_suite", quarantine_df, "silver", "tracks_quarantine", context))

        # Gold Layer Execution (2 suites)
        if layer in ("gold", "all"):
            enr_df = context.get("gold_enriched_chart_data") if "gold_enriched_chart_data" in context else self._read_dataset("gold", "enriched_chart_data")
            cntry_df = context.get("gold_country_analytics") if "gold_country_analytics" in context else self._read_dataset("gold", "country_analytics")

            if enr_df is not None: context["gold_enriched_chart_data"] = enr_df
            if cntry_df is not None: context["gold_country_analytics"] = cntry_df

            results.append(self.validate_suite("gold_enriched_charts_suite", enr_df, "gold", "enriched_chart_data", context))
            results.append(self.validate_suite("gold_country_analytics_suite", cntry_df, "gold", "country_analytics", context))

        # Marts Layer Execution (1 suite)
        if layer in ("marts", "all"):
            fact_chart = context.get("fact_chart") if "fact_chart" in context else self._read_dataset("core", "fact_chart")
            dim_track = context.get("dim_track") if "dim_track" in context else self._read_dataset("core", "dim_track")
            dim_country = context.get("dim_country") if "dim_country" in context else self._read_dataset("core", "dim_country")
            dim_date = context.get("dim_date") if "dim_date" in context else self._read_dataset("core", "dim_date")
            bridge_artist = context.get("bridge_track_artist") if "bridge_track_artist" in context else self._read_dataset("core", "bridge_track_artist")
            bridge_genre = context.get("bridge_track_genre") if "bridge_track_genre" in context else self._read_dataset("core", "bridge_track_genre")

            if fact_chart is not None: context["fact_chart"] = fact_chart
            if dim_track is not None: context["dim_track"] = dim_track
            if dim_country is not None: context["dim_country"] = dim_country
            if dim_date is not None: context["dim_date"] = dim_date
            if bridge_artist is not None: context["bridge_track_artist"] = bridge_artist
            if bridge_genre is not None: context["bridge_track_genre"] = bridge_genre

            results.append(self.validate_suite("referential_integrity_suite", fact_chart, "marts", "star_schema_integrity", context))

        return results

    def save_validation_results(self, results: list[SuiteValidationResult]) -> Path:
        """Persist structured validation metrics to data_quality/reports/great_expectations_validation.json."""
        payload = {
            "validation_run_utc": datetime.now(timezone.utc).isoformat(),
            "author": "Member 4 (Data Quality & Workflow Orchestration)",
            "summary": {
                "total_suites": len(results),
                "passed_suites": sum(1 for r in results if r.success),
                "failed_suites": sum(1 for r in results if not r.success),
                "total_expectations": sum(r.total_expectations for r in results),
                "passed_expectations": sum(r.passed_expectations for r in results),
                "failed_expectations": sum(r.failed_expectations for r in results),
                "overall_success": all(r.success for r in results) if results else False,
            },
            "suites": [r.to_dict() for r in results],
        }
        out_file = self.reports_dir / "great_expectations_validation.json"
        out_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return out_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Great Expectations Validator for Spotify Lakehouse (Data Quality)")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "marts", "all"], default="all")
    parser.add_argument("--data-dir", help="Path to lakehouse data directory")
    args = parser.parse_args()

    validator = GreatExpectationsValidator(data_dir=args.data_dir)
    print(f"Running Data Quality Great Expectations validation for layer: '{args.layer}'...")
    results = validator.run_layer_validation(args.layer)

    json_path = validator.save_validation_results(results)
    passed_suites = sum(1 for r in results if r.success)
    total_suites = len(results)
    print(f"Validation complete: {passed_suites}/{total_suites} suites passed.")
    print(f"Validation JSON saved to: {json_path}")

    try:
        from data_quality.quality.report_generator import generate_validation_reports
    except ImportError:
        try:
            from member4.quality.report_generator import generate_validation_reports
        except ImportError:
            from quality.report_generator import generate_validation_reports

    html_path = generate_validation_reports(results)
    print(f"Reports generated at: {html_path}")

    return 0 if all(r.success for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
