{% set year_value = env_var('CURRENT_YEAR') %}
{% set month_value = env_var('CURRENT_MONTH') %}

{{config({
    "materialized":"incremental",
    "unique_key":"base64_event",
    "schema":"snowplow_" + year_value|string + '_' + month_value|string, 
  })
}}

WITH base as (

    SELECT *
    FROM {{ source('fishtown_snowplow', 'bad_events') }}
    WHERE length(JSONTEXT['errors']) > 0
      AND date_part(month, uploaded_at::timestamp) = '{{ month_value }}'
      AND date_part(year, uploaded_at::timestamp) = '{{ year_value }}'

    {%- if is_incremental() %}
      AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif -%}

), renamed AS (

    SELECT DISTINCT JSONTEXT [ 'line' ] :: string              AS base64_event,
                    TO_ARRAY(JSONTEXT [ 'errors' ])            AS error_array,
                    JSONTEXT [ 'failure_tstamp' ] :: timestamp AS failure_timestamp,
                    'Fishtown'                                 AS infra_source,
                    uploaded_at
    FROM base

)

SELECT *
FROM renamed
ORDER BY failure_timestamp