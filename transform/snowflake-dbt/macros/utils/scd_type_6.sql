{%- macro scd_type_6(primary_key, primary_key_raw, source_cte, source_timestamp, casted_cte) -%}

, max_by_primary_key AS (
  SELECT
    {{ primary_key_raw }} AS primary_key,
    MAX(IFF(max_task_instance IN ( SELECT MAX(max_task_instance) FROM {{ source_cte }}), 1, 0)) AS is_in_most_recent_task,
    MAX(DATEADD('sec', _uploaded_at, '1970-01-01')) AS uploaded_at
  FROM {{ source_cte }}
  GROUP BY 1

), windowed AS (
  SELECT
    {{casted_cte}}.*,

    {{ source_timestamp }} AS valid_from,
    COALESCE(
      DATEADD('millisecond', -1, FIRST_VALUE({{ source_timestamp }}) OVER (
        PARTITION BY {{casted_cte}}.{{primary_key}}
        ORDER BY {{ source_timestamp }}
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
      ),
      IFF(is_in_most_recent_task = FALSE, max_by_primary_key.uploaded_at, NULL)
    ) AS valid_to,
    (valid_to IS NULL) AS is_currently_valid

  FROM {{casted_cte}}
    LEFT JOIN max_by_primary_key
      ON renamed.{{primary_key}} = max_by_primary_key.primary_key
  ORDER BY valid_from, valid_to

)

SELECT *
FROM windowed

{%- endmacro -%}