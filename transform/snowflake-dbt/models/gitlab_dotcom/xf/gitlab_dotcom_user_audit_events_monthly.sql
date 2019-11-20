{{ config({
    "materialized": "incremental",
    "unique_key": "user_month_unique_id"
    })
}}

WITH months AS (

    SELECT DISTINCT
      first_day_of_month AS skeleton_month
    FROM {{ ref('date_details') }}
    WHERE first_day_of_month < CURRENT_DATE
    {% if is_incremental() %}
      AND first_day_of_month >= (SELECT MAX(audit_event_month) from {{ this }})
    {% endif %}

), users AS (

    SELECT
      user_id,
      DATE_TRUNC(month, user_created_at) AS user_created_at_month
    FROM {{ ref('gitlab_dotcom_users') }}
    WHERE TO_DATE(created_at) < DATE_TRUNC('month', CURRENT_DATE)

), skeleton AS ( -- Create a framework of one row per user per month (after their creation date)
    SELECT
      users.user_id,
      users.user_created_at_month,
      months.skeleton_month,
      DATEDIFF(month, users.user_created_at_month, months.skeleton_month) AS months_since_join_date
    FROM users
      LEFT JOIN months
        ON DATE_TRUNC('month', user_created_at_month) <= months.skeleton_month

), audit_events AS (

    SELECT
      author_id,
      DATE_TRUNC(month, audit_event_created_at)  AS audit_event_month,
      COUNT(*)                                   AS audit_events_count
    FROM {{ ref('gitlab_dotcom_audit_events') }}
    {% if is_incremental() %}
    WHERE created_at >= (SELECT MAX(audit_event_month) from {{ this }})
    {% endif %}
    GROUP BY 1,2

), joined AS (

    SELECT
      skeleton.user_id,
      skeleton.user_created_at_month,
      skeleton.skeleton_month                                    AS audit_event_month,
      skeleton.months_since_join_date,
      COALESCE(audit_events.audit_events_count, 0)               AS audit_events_count,
      IFF(audit_events.audit_events_count > 0, TRUE, FALSE)      AS user_was_active_in_month,
      {{ dbt_utils.surrogate_key('user_id', 'skeleton_month') }} AS user_month_unique_id
    FROM skeleton
      LEFT JOIN audit_events
        ON skeleton.user_id = audit_events.author_id
        AND skeleton.skeleton_month = audit_events.audit_event_month
    ORDER BY
      skeleton.user_id,
      skeleton.skeleton_month

)

SELECT *
FROM joined
