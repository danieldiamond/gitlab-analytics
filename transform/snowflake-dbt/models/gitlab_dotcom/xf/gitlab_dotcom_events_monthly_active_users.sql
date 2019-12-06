{{ config({
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
    {% if is_incremental() %}
      AND date_day >= DATEADD('day', -7, (SELECT MAX(day) FROM {{ this }}) )
    {% endif %}

), audit_events AS (

    SELECT DISTINCT
      author_id,
      TO_DATE(created_at) AS audit_event_day
    FROM {{ ref('gitlab_dotcom_audit_events') }}
    WHERE TRUE
    {% if is_incremental() %}
      AND created_at >= DATEADD('day', -36, (SELECT MAX(day) FROM {{ this }}) )
    {% endif %}

), events AS (

    SELECT DISTINCT
      author_id,
      TO_DATE(created_at) AS event_day
    FROM {{ ref('gitlab_dotcom_events') }}
    WHERE TRUE
    {% if is_incremental() %}
      AND created_at >= DATEADD('day', -36, (SELECT MAX(day) FROM {{ this }}) )
    {% endif %}

), audit_events_active_user AS (

    SELECT
      days.day,
      days.is_last_day_of_month,
      COUNT(DISTINCT author_id)   AS count_audit_events_active_users_last_28_days
    FROM days
      INNER JOIN audit_events
        ON audit_event_day BETWEEN DATEADD('day', -27, days.day) AND days.day
    GROUP BY
      days.day,
      days.is_last_day_of_month
    ORDER BY
      days.day

), events_active_user AS (

    SELECT
      days.day,
      days.is_last_day_of_month,
      COUNT(DISTINCT author_id)   AS count_events_active_users_last_28_days
    FROM days
      INNER JOIN events
        ON events.event_day BETWEEN DATEADD('day', -27, days.day) AND days.day
    GROUP BY
      days.day,
      days.is_last_day_of_month
    ORDER BY
      days.day

), joined AS (
  
    SELECT
     audit_events_active_user.day,
     audit_events_active_user.is_last_day_of_month,
     audit_events_active_user.count_audit_events_active_users_last_28_days,
     events_active_user.count_events_active_users_last_28_days
    FROM audit_events_active_user
      LEFT JOIN events_active_user 
        ON audit_events_active_user.day = events_active_user.day 
)

SELECT *
FROM joined
