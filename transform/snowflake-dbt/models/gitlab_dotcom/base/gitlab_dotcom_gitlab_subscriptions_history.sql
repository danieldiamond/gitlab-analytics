WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions_history') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                                      AS gitlab_subscription_history_id,
      gitlab_subscription_created_at::TIMESTAMP        AS gitlab_subscription_created_at,
      gitlab_subscription_updated_at::TIMESTAMP        AS gitlab_subscription_updated_at,
      start_date::TIMESTAMP                            AS start_date,
      end_date::TIMESTAMP                              AS end_date,
      trial_starts_on::TIMESTAMP                       AS trial_starts_on,
      trial_ends_on::TIMESTAMP                         AS trial_ends_on,
      namespace_id::INTEGER                            AS namespace_id,
      hosted_plan_id::INTEGER                          AS hosted_plan_id,
      max_seats_used::INTEGER                          AS max_seats_used,
      seats::INTEGER                                   AS seats,
      trial::BOOLEAN                                   AS is_trial,
      change_type                                      AS todo,
      gitlab_subscription_id::INTEGER                  AS gitlab_subscription_id,
      created_at::TIMESTAMP                            AS gitlab_subscription_history_created_at

    FROM source

)

SELECT *
FROM renamed
