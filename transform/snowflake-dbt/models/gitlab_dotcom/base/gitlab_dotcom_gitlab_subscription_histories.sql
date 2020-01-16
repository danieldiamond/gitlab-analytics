WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'gitlab_subscription_histories') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1 -- Each ID should always have the same data since it's a log table.

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
      change_type::INTEGER                             AS change_type,
      gitlab_subscription_id::INTEGER                  AS gitlab_subscription_id,
      created_at::TIMESTAMP                            AS created

    FROM source

)

SELECT *
FROM renamed
