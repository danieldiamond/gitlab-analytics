WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'alert_management_alerts') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER                AS alert_management_alert_id,
    created_at::TIMESTAMP_NTZ  AS created_at,
    updated_at::TIMESTAMP_NTZ  AS updated_at,
    started_at::TIMESTAMP_NTZ  AS started_at,
    ended_at::TIMESTAMP_NTZ    AS ended_at,
    events::INTEGER            AS alert_management_alert_events,
    iid::INTEGER               AS alert_management_alert_iid,
    severity::INTEGER          AS severity_id,
    status::INTEGER            AS status_id,
    issue_id::INTEGER          AS issue_id,
    project_id::INTEGER        AS project_id,
    title::VARCHAR             AS alert_management_alert_title,
    description::VARCHAR       AS alert_management_alert_description,
    service::VARCHAR           AS alert_management_alert_service,
    monitoring_tool::VARCHAR   AS monitoring_tool

  FROM source

)

SELECT *
FROM renamed
