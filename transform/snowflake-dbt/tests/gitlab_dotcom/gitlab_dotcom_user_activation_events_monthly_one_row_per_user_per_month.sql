--Fail if a user doesn't have a row for a specific month after their signup

{% set tables_list = ['gitlab_dotcom_user_activation_events_monthly',
                       'gitlab_dotcom_user_issue_created_monthly',
                       'gitlab_dotcom_user_merge_request_opened_monthly',
                       'gitlab_dotcom_user_project_created_monthly'
] %}

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
    WHERE created_at < date_trunc('month', CURRENT_DATE)::DATE

), skeleton AS ( -- Create a framework of one row per user per month (after their creation date)

    SELECT
      users.user_id,
      users.user_created_at_month,
      months.skeleton_month

    FROM users
    LEFT JOIN months
      ON DATE_TRUNC('month', users.user_created_at_month) <= months.skeleton_month

), skeleton_test AS (

  {%- for table in tables_list %}
  (
    SELECT
    skeleton.user_id AS skeleton_user_id,
    skeleton.skeleton_month AS skeleton_month,
    event_table.event_month AS event_month

    FROM skeleton
    LEFT JOIN {{ ref( table ) }} AS event_table
      ON skeleton.skeleton_month = event_table.event_month
        AND skeleton.user_id = event_table.user_id

    {%- if not loop.last %}
    )
    UNION

    {%- else -%}
    )
    {% endif %}
  {% endfor -%}
)

SELECT
*
FROM skeleton_test
WHERE event_month IS NULL
