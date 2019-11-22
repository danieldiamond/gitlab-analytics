{%- macro scd_type_6(primary_key, cte_name, source) -%}

, max_uploaded_at_by_id AS (
  SELECT
    namespace_id,
    MAX(DATEADD('sec', _uploaded_at, '1970-01-01')::DATE) AS uploaded_at
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }} -- Source table
  GROUP BY 1

), windowed AS (
  SELECT
    renamed.*,

    FIRST_VALUE(updated_at) OVER (
        PARTITION BY renamed.namespace_id
        ORDER BY updated_at
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ) AS next_updated_at,
    updated_at AS valid_from,
    CASE
      WHEN next_updated_at IS NOT NULL THEN DATEADD('millisecond', -1, next_updated_at)
      WHEN is_in_most_recent_task = FALSE THEN max_uploaded_at_by_id.uploaded_at
    END AS valid_to,
    CASE
      WHEN next_updated_at IS NOT NULL THEN FALSE
      WHEN is_in_most_recent_task = FALSE THEN FALSE
      ELSE TRUE
    END AS is_currently_valid

  FROM renamed
    LEFT JOIN max_uploaded_at_by_id
      ON renamed.namespace_id = max_uploaded_at_by_id.namespace_id
  ORDER BY updated_at
)

SELECT *
FROM windowed

{%- endmacro -%}
