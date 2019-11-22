{%- macro scd_type_6(primary_key, source_cte, casted_cte) -%}

, max_by_primary_key AS (
  SELECT
    {{ primary_key }} AS primary_key,
    MAX(IFF(_task_instance IN ( SELECT MAX(_task_instance) FROM source), 1, 0)) AS is_in_most_recent_task,
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
    
    COALESCE(
      DATEADD('millisecond', -1, FIRST_VALUE(updated_at) OVER (
        PARTITION BY {{casted_cte}}.{{primary_key}}
        ORDER BY updated_at
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
      ),
      IFF(is_in_most_recent_task = FALSE, max_uploaded_at_by_primary_key.uploaded_at, NULL) -- always needed?
    ) AS valid_to,
    (valid_to IS NOT NULL) AS is_currently_valid

  FROM {{casted_cte}}
    LEFT JOIN max_by_primary_key
      ON renamed.namespace_id = max_by_primary_key.primary_key
  ORDER BY valid_from, valid_to

)

SELECT *
FROM windowed

{%- endmacro -%}