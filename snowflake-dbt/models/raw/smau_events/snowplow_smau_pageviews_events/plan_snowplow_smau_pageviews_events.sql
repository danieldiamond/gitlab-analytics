{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = [
   {
      "event_name":"board_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/boards\/[0-9]{1,}",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"epic_list_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/epics(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"epic_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/epics\/[0-9]{1,}",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"issue_list_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/issues(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"issue_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/issues\/[0-9]{1,}",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"label_list_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/labels(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"milestones_list_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/milestones(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"milestone_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/milestones\/[0-9]{1,}",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"notification_settings_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"\/profile\/notifications(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"personal_issues_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"\/dashboard\/issues(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"roadmap_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/roadmap(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"todo_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"\/dashboard\/todos(\/)?",
            "regexp_function":"REGEXP"
         }
      ]
   }
]

-%}

WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id
  FROM {{ ref('snowplow_page_views_all')}}
  WHERE page_view_start >= '2019-01-01'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

{% for event_cte in event_ctes %}

, {{ smau_events_ctes(event_name=event_cte.event_name, regexp_where_statements=event_cte.regexp_where_statements) }}

{% endfor -%}

, unioned AS (

  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte.event_name }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}

)

, unioned AS (
  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte.event_name }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
