-- Fail if there are duplicated rows for the tuple (user_id, event_month)

{% set tables_list = ['gitlab_dotcom_user_activation_events_monthly',
                       'gitlab_dotcom_user_issue_created_monthly',
                       'gitlab_dotcom_user_merge_request_opened_monthly',
                       'gitlab_dotcom_user_project_created_monthly'
] %}


WITH row_count_calc AS (

  {%- for table in tables_list %}
  (
    SELECT
      user_id,
      activity_name,
      event_month,
      COUNT(*) AS row_count
    FROM {{ ref( table ) }}
    GROUP BY 1,2,3

    {%- if not loop.last %}
    )
    UNION

    {%- else -%}
    )
    {% endif %}

  {% endfor -%}
)

SELECT *
FROM row_count_calc
WHERE row_count > 1
