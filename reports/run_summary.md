# Pipeline run summary

- run_id: `20261103T040232Z`
- status: **SUCCESS**
- total time: 83.2s

```
================================================================================================================
LAYER  DATASET                    ROWS IN   ROWS OUT  QUARANTINED  DUPS REMOVED  NOTE
----------------------------------------------------------------------------------------------------------------
bronze tracks                     114,000    114,000            0             0  all columns stored as string; + lineage column
bronze charts                      50,000     50,000            0             0  all columns stored as string; + lineage column
bronze countries                      249        249            0             0  all columns stored as string; + lineage column
silver tracks                     114,000     89,740            1        24,259  one row per track_id; genres kept in array
silver track_genres               113,999    113,549            0             0  bridge: track_id x genre
silver charts                      50,000     50,000            0             0  typed, validated, track_id extracted
silver countries                      249        249            0             0  ISO codes preserved
silver enriched_chart_data         50,000     50,000            0             0  LEFT JOIN tracks+countries; track match 17.11%
gold   enriched_chart_data         50,000     50,000            0             0  partitioned by chart_year; + date parts + char
gold   music_analytics             50,000     50,000            0             0  curated wide BI table; one row per chart entry
gold   country_analytics                -         73            0             0  grain: region x chart_type
gold   artist_analytics                 -      3,363            0             0  grain: credited artist (comma-split)
gold   genre_analytics                  -        114            0             0  grain: genre; catalogue + chart metrics
gold   monthly_analytics                -          8            0             0  grain: year-month x chart_type
----------------------------------------------------------------------------------------------------------------
Stage times: bronze=11.7s, silver=44.5s, gold=21.3s, verify=5.7s
================================================================================================================
```