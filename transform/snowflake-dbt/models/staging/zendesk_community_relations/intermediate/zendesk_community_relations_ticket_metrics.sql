WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_community_relations_ticket_metrics_source') }}

),

renamed AS (

    SELECT
      *,
      IFNULL(reply_time_in_minutes_during_calendar_hours, 0)  AS sla_reply_time_calendar_hours,
      IFNULL(reply_time_in_minutes_during_business_hours, 0)  AS sla_reply_time_business_hours

    FROM source

)

SELECT *
FROM renamed
