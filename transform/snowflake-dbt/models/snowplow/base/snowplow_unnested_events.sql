
WITH unioned AS (
        {{ dbt_utils.union_tables(
                tables =[ ref('snowplow_gitlab_events'), ref('snowplow_fishtown_unnested_events')],
                column_override= none,
                exclude = none,
                source_column_name= none
            ) }}
)

SELECT *
FROM unioned
ORDER BY dvce_created_tstamp