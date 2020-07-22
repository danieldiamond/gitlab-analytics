WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'prometheus_alerts') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                                     AS prometheus_alert_id,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,
      project_id::INTEGER                             AS project_id
    FROM source

)

SELECT *
FROM renamed
