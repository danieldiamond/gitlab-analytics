{%- macro scd_type_6(primary_key, source_cte, casted_cte) -%}

, max_uploaded_at_by_primary_key AS (
  SELECT
    {{ primary_key }} AS primary_key,
    MAX(DATEADD('sec', _uploaded_at, '1970-01-01')::DATE) AS uploaded_at
  FROM {{ source_cte }}
  GROUP BY 1

), windowed AS (
  SELECT
    {{casted_cte}}.*,

    FIRST_VALUE(updated_at) OVER (
        PARTITION BY {{casted_cte}}.{{primary_key}}
        ORDER BY updated_at
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ) AS next_updated_at,
    updated_at AS valid_from,
    CASE
      WHEN next_updated_at IS NOT NULL THEN DATEADD('millisecond', -1, next_updated_at)
      WHEN is_in_most_recent_task = FALSE THEN max_uploaded_at_by_primary_key.uploaded_at
    END AS valid_to,
    CASE
      WHEN next_updated_at IS NOT NULL THEN FALSE
      WHEN is_in_most_recent_task = FALSE THEN FALSE
      ELSE TRUE
    END AS is_currently_valid

  FROM {{casted_cte}}
    LEFT JOIN max_uploaded_at_by_primary_key
      ON {{casted_cte}}.{{primary_key}} = max_uploaded_at_by_primary_key.{{primary_key}}
  ORDER BY updated_at

)

SELECT *
FROM windowed

{%- endmacro -%}
