{{ config({
    "materialized": "incremental",
    "unique_key": "resource_milestone_event_id"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'resource_milestone_events') }}
    {% if is_incremental() %}
      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
    {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

)

, renamed AS (

    SELECT
      id                                             AS resource_milestone_event_id,
      action::INTEGER                                AS action_type_id,
      {{ resource_event_action_type('action') }}     AS action_type,
      user_id::INTEGER                               AS user_id,
      issue_id::INTEGER                              AS issue_id,
      merge_request_id::INTEGER                      AS merge_request_id,
      milestone_id::INTEGER                          AS milestone_id,
      {{ map_state_id('state_id') }}                 AS state,

      created_at::TIMESTAMP                          AS created_at
    FROM source

)

SELECT *
FROM renamed
