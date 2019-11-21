{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'subscriptions') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER               AS subscription_id,
      user_id::INTEGER          AS user_id,
      subscribable_id::INTEGER  AS subscribable_id,
      project_id::INTEGER       AS project_id,
      subscribable_type,
      subscribed::BOOLEAN       AS is_subscribed,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at

    FROM source

)

SELECT  *
FROM renamed
