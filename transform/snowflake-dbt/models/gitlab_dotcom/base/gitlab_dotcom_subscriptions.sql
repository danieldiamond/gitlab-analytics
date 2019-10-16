{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'subscriptions') }}

), renamed AS (

    SELECT

      id::INTEGER               AS subscription_id,
      user_id::INTEGER          AS user_id,
      subscribable_id::INTEGER  AS subscribable_id,
      project_id::INTEGER       AS project_id,
      subscribable_type,
      subscribed::BOOLEAN       AS is_subscribed,
      created_at::TIMESTAMP     AS subscription_created_at,
      updated_at::TIMESTAMP     AS subscription_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT  *
FROM renamed
