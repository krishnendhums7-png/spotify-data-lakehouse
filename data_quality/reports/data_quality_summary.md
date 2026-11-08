# Data Quality Validation Summary (Data Quality)
*Generated: 2026-11-08 17:03:04 UTC*

## Executive Summary
- **Overall Quality Status:** PASSED
- **Expectation Success Rate:** `100.0%` (40/40 satisfied)
- **Suites Passed:** `5/5`
- **Engines:** Native Great Expectations 1.24.1 + Custom Integration Validator

## Expectation Suite Results (5 Suites)
| Suite Name | Layer | Asset | Engine | Total Checks | Passed | Failed | Status |
|---|---|---|---|:---:|:---:|:---:|:---:|
| `silver_tracks_suite` | SILVER | `tracks` | GreatExpectations_1.24 | 19 | 19 | 0 | PASS |
| `silver_charts_suite` | SILVER | `charts` | GreatExpectations_1.24 | 11 | 11 | 0 | PASS |
| `silver_countries_suite` | SILVER | `countries` | GreatExpectations_1.24 | 5 | 5 | 0 | PASS |
| `silver_track_genres_suite` | SILVER | `track_genres` | GreatExpectations_1.24 | 3 | 3 | 0 | PASS |
| `quarantine_isolation_suite` | SILVER | `tracks_quarantine` | CustomIntegrationValidator | 2 | 2 | 0 | PASS |

## Enforced Quality Dimensions
1. **Schema & Lineage:** Exact column presence, data types, and lineage fields verified via Great Expectations.
2. **Null Constraints:** 0% nulls in primary keys (`track_id`, `chart_record_id`, `iso_alpha3`).
3. **Deduplication:** 100% uniqueness in primary and composite grains `(date, region, chart, rank)`.
4. **Value Ranges:** Popularity `[0, 100]`, Audio features `[0.0, 1.0]`, Loudness `[-60, 10]`, Tempo `(0, 300]`, Key `[0, 11]`, Mode `{0, 1}`.
5. **Dates & Seasons:** ISO 8601 formatting, valid months `1..12`, seasons in `{winter, spring, summer, autumn}`.
6. **Chart Positions:** Rank `1..200` for Top 200, `1..50` for Viral 50, trend categories.
7. **Referential Integrity (Custom Integration Check):** Foreign keys verified across fact and dimension tables; bridge weights sum strictly to 1.0.
8. **Quarantine Isolation (Custom Integration Check):** Malformed rows routed to quarantine with documented reasons, within 0.1% budget.

Interactive HTML report: `data_quality/reports/great_expectations_report.html`