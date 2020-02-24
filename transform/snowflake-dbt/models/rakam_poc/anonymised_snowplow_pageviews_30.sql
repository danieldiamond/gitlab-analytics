{%- set tables_to_import = [
                            'configure_snowplow_smau_pageviews_events',
                            'create_snowplow_smau_pageviews_events',
                            'manage_snowplow_smau_pageviews_events',
                            'monitor_snowplow_smau_pageviews_events',
                            'package_snowplow_smau_pageviews_events',
                            'plan_snowplow_smau_pageviews_events',
                            'release_snowplow_smau_pageviews_events'
]

-%}

WITH snowplow_page_views_30 AS (
  
  SELECT *
  FROM {{ ref('snowplow_page_views_30')}}

)

{% for table_to_import in tables_to_import %}

, {{table_to_import}} AS (
  
  SELECT *
  FROM {{ ref( table_to_import )}}
  
)

{% endfor -%}

, unioned AS (

    {% for table_to_import in tables_to_import %}

      SELECT *
      FROM {{ table_to_import }}

      {%- if not loop.last %}
        UNION
      {%- endif %}

    {% endfor -%}

)

, filtered_pageviews AS (
  
  SELECT snowplow_page_views_30.*
  FROM snowplow_page_views_30
  INNER JOIN unioned 
    ON snowplow_page_views_30.page_view_id = unioned.event_surrogate_key
  
)

SELECT *
FROM filtered_pageviews
