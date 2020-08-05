WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'alert_management_alerts') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
        id::NUMBER                AS alert_management_alert_id,
        created_at::TIMESTAMP_NTZ  AS created_at,
        updated_at::TIMESTAMP_NTZ  AS updated_at,
        started_at::TIMESTAMP_NTZ  AS started_at,
        ended_at::TIMESTAMP_NTZ    AS ended_at,
        events::NUMBER            AS alert_management_alert_events,
        iid::NUMBER               AS alert_management_alert_iid,
        severity::NUMBER          AS severity_id,
        status::NUMBER            AS status_id,
        issue_id::NUMBER          AS issue_id,
        project_id::NUMBER        AS project_id,
        service::VARCHAR           AS alert_management_alert_service,
        monitoring_tool::VARCHAR   AS monitoring_tool

    FROM source

)

SELECT *
FROM renamed
