{{ config({
    "materialized": "table",
    "schema": "staging"
    })
}}

WITH months AS (

    SELECT DISTINCT
      first_day_of_month AS skeleton_month

    FROM {{ ref('date_details') }}
    WHERE first_day_of_month < DATE_TRUNC('month', CURRENT_DATE)

  ), users AS (

    SELECT
      user_id,
      DATE_TRUNC(month, created_at) AS user_created_at_month

    FROM {{ ref('gitlab_dotcom_users') }}
    WHERE created_at < DATE_TRUNC('month', CURRENT_DATE)

), skeleton AS ( -- Create a framework of one row per user per month (after their creation date)

    SELECT
      users.user_id,
      users.user_created_at_month,
      months.skeleton_month,
      'project_created' AS activity_name,
      DATEDIFF(month, users.user_created_at_month, months.skeleton_month)
                        AS months_since_join_date

    FROM users
    LEFT JOIN months
      ON DATE_TRUNC('month', users.user_created_at_month) <= months.skeleton_month

), projects AS (

    SELECT
      creator_id                              AS author_id,
      DATE_TRUNC('month', created_at)         AS event_month,
      COUNT(*)                                AS events_count

    FROM {{ ref('gitlab_dotcom_projects') }}
    GROUP BY 1,2

), joined AS (

  SELECT
    skeleton.user_id,
    skeleton.user_created_at_month,
    skeleton.skeleton_month                     AS event_month,
    skeleton.months_since_join_date,
    skeleton.activity_name,
    COALESCE(projects.events_count, 0)          AS events_count,
    IFF(projects.events_count > 0, TRUE, FALSE) AS user_was_active_in_month

  FROM skeleton
  LEFT JOIN projects
    ON skeleton.user_id = projects.author_id
    AND skeleton.skeleton_month = projects.event_month
  ORDER BY
    skeleton.user_id,
    skeleton.skeleton_month

)

SELECT *
FROM joined
