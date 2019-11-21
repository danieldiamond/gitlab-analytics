{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}
  WHERE id != 572635 -- This ID has NULL values for many of the important columns.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
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
         SELECT MAX(_task_instance) FROM source)    AS is_deleted

    FROM source

)

SELECT *
FROM renamed
