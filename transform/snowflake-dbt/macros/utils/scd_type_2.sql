{%- macro scd_type_2(primary_key_renamed, primary_key_raw, source_cte='distinct_source', source_timestamp='valid_from', casted_cte='renamed') -%}

, max_by_primary_key AS (
  SELECT
    {{ primary_key_raw }} AS primary_key,
    MAX(IFF(max_task_instance IN ( SELECT MAX(max_task_instance) FROM {{ source_cte }} ), 1, 0)) AS is_in_most_recent_task,
    MAX({{ source_timestamp }} ) AS max_timestamp
  FROM {{ source_cte }}
  GROUP BY 1

), windowed AS (
  SELECT
    {{casted_cte}}.*,

    COALESCE(
      DATEADD('millisecond', -1, FIRST_VALUE( {{ source_timestamp }} ) OVER (
        PARTITION BY {{casted_cte}}.{{primary_key_renamed}}
        ORDER BY {{ source_timestamp }}
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
      ),
      IFF(is_in_most_recent_task = FALSE, max_by_primary_key.max_timestamp, NULL)
    ) AS valid_to,
    (valid_to IS NULL) AS is_currently_valid

  FROM {{casted_cte}}
    LEFT JOIN max_by_primary_key
      ON renamed.{{primary_key_renamed}} = max_by_primary_key.primary_key
  ORDER BY {{ source_timestamp }}, valid_to

)

SELECT *
FROM windowed

{%- endmacro -%}
