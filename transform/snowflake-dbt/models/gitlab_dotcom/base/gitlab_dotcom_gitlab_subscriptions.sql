{{ config({
    "schema": "staging"
    })
}}

WITH
{{ distinct_source(source=source('gitlab_dotcom', 'gitlab_subscriptions'))}}

, renamed AS (

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
      valid_from -- Column was added in distinct_source CTE
  
    FROM distinct_source
    WHERE gitlab_subscription_id != 572635 -- This ID has NULL values for many of the important columns.

)

/* Note: the primary key used is namespace_id, not subscription id.
   This matches our business use case better. */
{{ scd_type_2(
    primary_key_renamed='namespace_id', 
    primary_key_raw='namespace_id'
) }}
