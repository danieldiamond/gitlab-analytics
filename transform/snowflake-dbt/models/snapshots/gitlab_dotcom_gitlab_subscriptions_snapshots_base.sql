{{ config({
    "schema": "staging",
    "alias": "gitlab_dotcom_gitlab_subscriptions_snapshots"
    })
}}

WITH source AS (

  SELECT 
    *
  FROM {{ source('snapshots', 'gitlab_dotcom_gitlab_subscriptions_snapshots') }}
  WHERE id != 572635 -- This ID has NULL values for many of the important columns.

), renamed AS (

  SELECT
    dbt_scd_id::VARCHAR                           AS gitlab_subscription_snapshot_id,
    id::integer                                   AS gitlab_subscription_id,
    start_date::date                              AS gitlab_subscription_start_date,
    end_date::date                                AS gitlab_subscription_end_date,
    trial_ends_on::date                           AS gitlab_subscription_trial_ends_on,
    namespace_id::integer                         AS namespace_id,
    hosted_plan_id::integer                       AS plan_id,
    max_seats_used::integer                       AS max_seats_used,
    seats::integer                                AS seats,
    trial::boolean                                AS is_trial,
    created_at::timestamp                         AS gitlab_subscription_created_at,
    updated_at::timestamp                         AS gitlab_subscription_updated_at,
    "DBT_VALID_FROM"::TIMESTAMP                   AS valid_from,
    "DBT_VALID_TO"::TIMESTAMP                     AS valid_to
  
  FROM source
    
)

SELECT *
FROM renamed
