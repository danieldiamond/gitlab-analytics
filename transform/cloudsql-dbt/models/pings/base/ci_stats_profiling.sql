{{
  config({
    "materialized": "table",
    "post-hook": [
      create_index(this, "id"),
      create_index(this, "branch"),
      create_index(this, "commit"),
      create_index(this, "updated_at")
    ]
  })
}}

SELECT *
FROM ci_stats.spec_profiling_results

