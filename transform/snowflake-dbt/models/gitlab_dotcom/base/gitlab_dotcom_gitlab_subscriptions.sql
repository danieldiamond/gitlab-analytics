{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}
  WHERE id != 572635 -- This ID has NULL values for many of the important columns.

), renamed AS (

    SELECT DISTINCT
      id::INTEGER                                   AS gitlab_subscription_id,
      start_date::DATE                              AS gitlab_subscription_start_date,
      end_date::DATE                                AS gitlab_subscription_end_date,
      trial_ends_on::DATE                           AS gitlab_subscription_trial_ends_on,
      namespace_id::INTEGER                         AS namespace_id,
      hosted_plan_id::INTEGER                       AS plan_id,
      max_seats_used::INTEGER                       AS max_seats_used,
      seats::INTEGER                                AS seats,
      trial::BOOLEAN                                AS is_trial,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at,
       _task_instance NOT IN (
         SELECT MAX(_task_instance) FROM source)    AS is_in_most_recent_task

    FROM source

), max_uploaded_at_by_id AS (
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
