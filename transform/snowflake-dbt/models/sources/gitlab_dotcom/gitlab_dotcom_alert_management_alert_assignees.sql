WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'alert_management_alert_assignees') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                AS alert_management_alert_assignees,
      user_id::INTEGER           AS user_id,
      alert_id::INTEGER          AS alert_id

    FROM source

)

SELECT *
FROM renamed
