WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'subscriptions') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER               AS subscription_id,
      user_id::NUMBER          AS user_id,
      subscribable_id::NUMBER  AS subscribable_id,
      project_id::NUMBER       AS project_id,
      subscribable_type,
      subscribed::BOOLEAN       AS is_subscribed,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at

    FROM source

)

SELECT  *
FROM renamed
