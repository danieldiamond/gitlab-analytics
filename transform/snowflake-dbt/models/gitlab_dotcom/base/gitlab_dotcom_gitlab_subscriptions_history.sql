WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions_history') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id
      gitlab_subscription_created_at
      gitlab_subscription_updated_at
      start_date
      end_date
      trial_starts_on
      trial_ends_on
      namespace_id
      hosted_plan_id
      max_seats_used
      seats
      trial
      change_type
      gitlab_subscription_id
      created_at

    FROM source

)

SELECT *
FROM renamed
