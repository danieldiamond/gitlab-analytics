{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}
  WHERE id != 572635 -- This ID has NULL values for many of the important columns.

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
      created_at::TIMESTAMP                         AS gitlab_subscription_created_at,
      updated_at::TIMESTAMP                         AS gitlab_subscription_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
