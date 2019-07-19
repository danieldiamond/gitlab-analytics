{{ config({
    "materialized": "table"
    })
}}

WITH months AS (

    SELECT DISTINCT
      first_day_of_month AS skeleton_month

    FROM {{ ref('date_details') }}
    WHERE first_day_of_month < CURRENT_DATE

), users AS (

    SELECT
      user_id,
      DATE_TRUNC(month, user_created_at) AS user_created_at_month

    FROM {{ ref('gitlab_dotcom_users') }}

), skeleton AS ( -- Create a framework of one row per user per month (after their creation date)

    SELECT
      users.user_id,
      users.user_created_at_month,
      months.skeleton_month,
      'merge_request_opened' AS activity_name,
      DATEDIFF(month, users.user_created_at_month, months.skeleton_month)
                             AS months_since_join_date

    FROM users
    LEFT JOIN months
      ON DATE_TRUNC('month', users.user_created_at_month) <= months.skeleton_month

), merge_requests AS (

    SELECT
      author_id,
      DATE_TRUNC('month', merge_request_created_at) AS event_month,
      COUNT(*) AS events_count

    FROM {{ ref('gitlab_dotcom_merge_requests') }}
    GROUP BY 1,2

), joined AS (

    SELECT
      skeleton.user_id,
      skeleton.user_created_at_month,
      skeleton.skeleton_month                           AS event_month,
      skeleton.months_since_join_date,
      skeleton.activity_name,
      COALESCE(merge_requests.events_count, 0)          AS events_count,
      IFF(merge_requests.events_count > 0, TRUE, FALSE) AS user_was_active_in_month

    FROM skeleton
    LEFT JOIN merge_requests
      ON skeleton.user_id = merge_requests.author_id
      AND skeleton.skeleton_month = merge_requests.event_month
    ORDER BY
      skeleton.user_id,
      skeleton.skeleton_month

)

SELECT *
FROM joined