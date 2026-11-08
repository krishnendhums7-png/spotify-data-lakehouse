"""Validation Report Generator for Great Expectations Data Docs & Markdown (Data Quality).

Generates:
1. data_quality/reports/great_expectations_report.html (Interactive executive Data Docs)
2. data_quality/reports/data_quality_summary.md (GitHub / Viva audit summary)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_quality.quality.validator import SuiteValidationResult


def get_project_root() -> Path:
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


def generate_validation_reports(
    results: list[SuiteValidationResult],
    output_html_path: Path | None = None,
) -> Path:
    out_html = output_html_path or (REPORTS_DIR / "great_expectations_report.html")

    total_suites = len(results)
    passed_suites = sum(1 for r in results if r.success)
    failed_suites = total_suites - passed_suites
    total_exp = sum(r.total_expectations for r in results)
    passed_exp = sum(r.passed_expectations for r in results)
    failed_exp = total_exp - passed_exp
    success_rate = round((passed_exp / total_exp * 100), 1) if total_exp > 0 else 100.0

    status_color = "#10b981" if failed_suites == 0 and total_suites > 0 else "#ef4444"
    status_text = "ALL QUALITY GATES PASSED" if failed_suites == 0 and total_suites > 0 else f"{failed_suites} SUITE(S) FAILED"

    # Build suites HTML
    suites_html = []
    for r in results:
        status_val = getattr(r, "status", "PASSED" if r.success else "FAILED")
        engine_val = getattr(r, "engine", "GreatExpectations_1.24")

        if status_val == "PASSED":
            badge_class = "badge-pass"
            badge_text = "PASSED"
        elif status_val == "MISSING_DATASET":
            badge_class = "badge-fail"
            badge_text = "MISSING DATASET"
        else:
            badge_class = "badge-fail"
            badge_text = "FAILED"

        pass_pct = round((r.passed_expectations / r.total_expectations * 100), 1) if r.total_expectations > 0 else 0

        rows_html = []
        for e in r.evaluations:
            row_badge = '<span class="status-pass">PASS</span>' if e.success else '<span class="status-fail">FAIL</span>'
            obs_str = str(e.observed_value)
            if len(obs_str) > 60:
                obs_str = obs_str[:57] + "..."
            dim_badge = f'<span class="dim-tag dim-{e.dimension}">{e.dimension}</span>'

            rows_html.append(f"""
            <tr>
                <td>{row_badge}</td>
                <td>{dim_badge}</td>
                <td><strong>{e.expectation_type}</strong><br><small class="text-muted">{e.description}</small></td>
                <td><code>{obs_str}</code></td>
                <td>{e.unexpected_count:,}</td>
                <td>{e.element_count:,}</td>
            </tr>
            """)

        suites_html.append(f"""
        <div class="suite-card">
            <div class="suite-header">
                <div>
                    <h3 class="suite-title">{r.suite_name}</h3>
                    <span class="suite-sub">Layer: <strong>{r.layer.upper()}</strong> | Asset: <code>{r.target_asset}</code> | Engine: <em>{engine_val}</em></span>
                </div>
                <div>
                    <span class="badge {badge_class}">{badge_text}</span>
                    <span class="suite-stat">{r.passed_expectations}/{r.total_expectations} ({pass_pct}%)</span>
                </div>
            </div>
            <table class="styled-table">
                <thead>
                    <tr>
                        <th style="width: 70px;">Status</th>
                        <th style="width: 110px;">Dimension</th>
                        <th>Expectation & Description</th>
                        <th>Observed Metric / Value</th>
                        <th style="width: 90px;">Violations</th>
                        <th style="width: 90px;">Elements</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows_html)}
                </tbody>
            </table>
        </div>
        """)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Great Expectations Data Docs - Spotify Lakehouse</title>
    <style>
        :root {{
            --bg: #0b0f19;
            --surface: #111827;
            --surface-card: #1f2937;
            --border: #374151;
            --text-primary: #f9fafb;
            --text-secondary: #9ca3af;
            --pass: #10b981;
            --fail: #ef4444;
            --brand: #1db954;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: var(--bg);
            color: var(--text-primary);
            line-height: 1.5;
            padding: 32px 16px;
        }}
        .container {{ max-width: 1280px; margin: 0 auto; }}
        header {{
            background: linear-gradient(135deg, #1f2937 0%, #111827 100%);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 28px 32px;
            margin-bottom: 28px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header-title h1 {{ font-size: 26px; font-weight: 700; color: var(--text-primary); }}
        .header-title p {{ color: var(--text-secondary); font-size: 14px; margin-top: 4px; }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}
        .kpi-card {{
            background-color: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 20px;
        }}
        .kpi-label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-secondary); }}
        .kpi-value {{ font-size: 32px; font-weight: 700; margin-top: 6px; }}
        .kpi-sub {{ font-size: 13px; color: var(--text-secondary); margin-top: 4px; }}
        .suite-card {{
            background-color: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            margin-bottom: 24px;
            overflow: hidden;
        }}
        .suite-header {{
            padding: 18px 24px;
            background-color: var(--surface-card);
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .suite-title {{ font-size: 18px; font-weight: 600; color: var(--text-primary); }}
        .suite-sub {{ font-size: 13px; color: var(--text-secondary); margin-top: 2px; display: inline-block; }}
        .suite-stat {{ font-size: 14px; color: var(--text-secondary); margin-left: 12px; }}
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 9999px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .badge-pass {{ background-color: rgba(16, 185, 129, 0.2); color: var(--pass); }}
        .badge-fail {{ background-color: rgba(239, 68, 68, 0.2); color: var(--fail); }}
        .status-pass {{ color: var(--pass); font-weight: 600; }}
        .status-fail {{ color: var(--fail); font-weight: 600; }}
        .dim-tag {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            background: #374151;
            color: #d1d5db;
        }}
        .styled-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        .styled-table th {{
            text-align: left;
            padding: 12px 18px;
            background-color: rgba(31, 41, 55, 0.5);
            color: var(--text-secondary);
            font-weight: 600;
            border-bottom: 1px solid var(--border);
        }}
        .styled-table td {{
            padding: 14px 18px;
            border-bottom: 1px solid var(--border);
            vertical-align: top;
        }}
        .styled-table tr:last-child td {{ border-bottom: none; }}
        code {{
            background: rgba(0, 0, 0, 0.3);
            padding: 2px 6px;
            border-radius: 4px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
            font-size: 12px;
        }}
        .text-muted {{ color: var(--text-secondary); font-size: 12px; }}
        .footer {{
            text-align: center;
            color: var(--text-secondary);
            font-size: 13px;
            margin-top: 40px;
            padding: 16px;
            border-top: 1px solid var(--border);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>Spotify Data Lakehouse &bull; Great Expectations Data Docs</h1>
                <p>Data Quality Assurance & Workflow Orchestration Audit</p>
            </div>
            <div>
                <span class="badge" style="font-size: 14px; padding: 8px 16px; background-color: {status_color}22; color: {status_color};">
                    {status_text}
                </span>
            </div>
        </header>

        <section class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Expectation Success Rate</div>
                <div class="kpi-value" style="color: {status_color};">{success_rate}%</div>
                <div class="kpi-sub">{passed_exp} of {total_exp} expectations satisfied</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Suites Executed</div>
                <div class="kpi-value">{total_suites}</div>
                <div class="kpi-sub">{passed_suites} passed | {failed_suites} failed</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Active Layers</div>
                <div class="kpi-value" style="font-size: 22px; padding-top: 8px;">Bronze &bull; Silver &bull; Gold &bull; Marts</div>
                <div class="kpi-sub">End-to-End Lakehouse Integrity</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Execution Engines</div>
                <div class="kpi-value" style="font-size: 18px; padding-top: 8px;">Great Expectations 1.24 + Integration</div>
                <div class="kpi-sub">Standard GX Suites + Custom Star-Schema Checks</div>
            </div>
        </section>

        <section>
            {''.join(suites_html)}
        </section>

        <footer class="footer">
            Spotify Lakehouse &bull; Data Quality Architecture &bull; Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
        </footer>
    </div>
</body>
</html>
"""
    out_html.write_text(html_content, encoding="utf-8")

    # Generate Markdown Summary
    md_file = REPORTS_DIR / "data_quality_summary.md"
    lines = [
        "# Data Quality Validation Summary (Data Quality)",
        f"*Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}*",
        "",
        "## Executive Summary",
        f"- **Overall Quality Status:** {'PASSED' if passed_suites == total_suites and total_suites > 0 else 'FAILED'}",
        f"- **Expectation Success Rate:** `{success_rate}%` ({passed_exp}/{total_exp} satisfied)",
        f"- **Suites Passed:** `{passed_suites}/{total_suites}`",
        f"- **Engines:** Native Great Expectations 1.24.1 + Custom Integration Validator",
        "",
        f"## Expectation Suite Results ({total_suites} Suites)",
        "| Suite Name | Layer | Asset | Engine | Total Checks | Passed | Failed | Status |",
        "|---|---|---|---|:---:|:---:|:---:|:---:|",
    ]
    for r in results:
        status_val = getattr(r, "status", "PASSED" if r.success else "FAILED")
        engine_val = getattr(r, "engine", "GreatExpectations_1.24")
        status_md = "PASS" if r.success else f"**{status_val}**"
        lines.append(f"| `{r.suite_name}` | {r.layer.upper()} | `{r.target_asset}` | {engine_val} | {r.total_expectations} | {r.passed_expectations} | {r.failed_expectations} | {status_md} |")

    lines.extend([
        "",
        "## Enforced Quality Dimensions",
        "1. **Schema & Lineage:** Exact column presence, data types, and lineage fields verified via Great Expectations.",
        "2. **Null Constraints:** 0% nulls in primary keys (`track_id`, `chart_record_id`, `iso_alpha3`).",
        "3. **Deduplication:** 100% uniqueness in primary and composite grains `(date, region, chart, rank)`.",
        "4. **Value Ranges:** Popularity `[0, 100]`, Audio features `[0.0, 1.0]`, Loudness `[-60, 10]`, Tempo `(0, 300]`, Key `[0, 11]`, Mode `{0, 1}`.",
        "5. **Dates & Seasons:** ISO 8601 formatting, valid months `1..12`, seasons in `{winter, spring, summer, autumn}`.",
        "6. **Chart Positions:** Rank `1..200` for Top 200, `1..50` for Viral 50, trend categories.",
        "7. **Referential Integrity (Custom Integration Check):** Foreign keys verified across fact and dimension tables; bridge weights sum strictly to 1.0.",
        "8. **Quarantine Isolation (Custom Integration Check):** Malformed rows routed to quarantine with documented reasons, within 0.1% budget.",
        "",
        "Interactive HTML report: `data_quality/reports/great_expectations_report.html`",
    ])
    md_file.write_text("\n".join(lines), encoding="utf-8")

    return out_html
