{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = [
   {
      "event_name":"mr_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2}\/merge_requests/[0-9]*",
            "regexp_function":"REGEXP"
         },
         {
            "regexp_pattern":"/-/ide/(.)*",
            "regexp_function":"NOT REGEXP"
         }
      ]
   },
   {
      "event_name":"project_viewed_in_ide",
      "regexp_where_statements":[
         {
            "regexp_pattern":"/-/ide/project/.*",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"repo_file_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/tree\/(.)*",
            "regexp_function":"REGEXP"
         },
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/wiki\/tree\/(.)*",
            "regexp_function":"NOT REGEXP"
         },
         {
            "regexp_pattern":"/-/ide/(.)*",
            "regexp_function":"NOT REGEXP"
         },
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/snippets/[0-9]{1,}",
            "regexp_function":"NOT REGEXP"
         }
      ]
   },
   {
      "event_name":"search_performed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"/search",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"snippet_created",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/snippets/new",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"snippet_edited",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/snippets/[0-9]*/edit",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"snippets_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"((\/([0-9A-Za-z_.-])*){2,})?\/snippets/[0-9]{1,}",
            "regexp_function":"REGEXP"
         }
      ]
   },
   {
      "event_name":"wiki_page_viewed",
      "regexp_where_statements":[
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/wikis(\/(([0-9A-Za-z_.-]|\%))*){1,2}",
            "regexp_function":"REGEXP"
         },
         {
            "regexp_pattern":"/-/ide/(.)*",
            "regexp_function":"NOT REGEXP"
         },
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/tree\/(.)*",
            "regexp_function":"NOT REGEXP"
         },
         {
            "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/wikis\/snippets\/(.)*",
            "regexp_function":"NOT REGEXP"
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
  WHERE TRUE
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


SELECT *
FROM unioned
