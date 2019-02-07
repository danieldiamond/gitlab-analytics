WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.subscriptions

), renamed AS (

    SELECT

      id :: integer               as subscription_id,
      user_id :: integer          as user_id,
      subscribable_id :: integer  as subscribable_id,
      project_id :: integer       as project_id,
      subscribable_type,
      subscribed :: boolean       as is_subscribed,
      created_at :: timestamp     as subscription_created_at,
      updated_at :: timestamp     as subscription_updated_at

    FROM source


)

SELECT *
FROM renamed