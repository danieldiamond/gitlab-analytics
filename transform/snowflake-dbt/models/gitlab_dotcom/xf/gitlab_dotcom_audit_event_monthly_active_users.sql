{{ config({
    "schema": "analytics",
    "materialized": "incremental",
    "unique_key": "day"
    })
}}

WITH days AS (

    SELECT DISTINCT
      date_day                       AS day,
      (date_day = last_day_of_month) AS is_last_day_of_month
    FROM {{ ref('date_details') }}
    WHERE date_day < CURRENT_DATE

), audit_events AS (

    SELECT DISTINCT
      author_id,
      TO_DATE(audit_event_created_at) AS audit_event_day
    FROM {{ ref('gitlab_dotcom_audit_events') }}
    WHERE True
    {% if is_incremental() %}
      AND audit_event_created_at >= DATEADD(-7, 'days', (SELECT MAX(day) FROM {{ this }}) )
    {% endif %}

), joined AS (

    SELECT
      days.day,
      days.is_last_day_of_month,
      COUNT(DISTINCT author_id)   AS count_active_users_last_28_days
    FROM days
      INNER JOIN audit_events
        ON audit_event_day BETWEEN DATEADD("day", -27, days.day) AND days.day
    GROUP BY 1,2
    ORDER BY 1

)

SELECT *
FROM joined
