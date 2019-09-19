{{config({
    "materialized":"incremental",
    "unique_key":"bad_event_surrogate",
    "schema":"staging"
  })
}}

WITH base AS (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'bad_events') }}
    WHERE length(JSONTEXT['errors']) > 0

    {%- if is_incremental() %}
      AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif -%}

), renamed AS (

    SELECT 
      JSONTEXT [ 'line' ] :: string               AS base64_event,
      TO_ARRAY(JSONTEXT [ 'errors' ])             AS error_array,
      JSONTEXT [ 'failure_tstamp' ] :: timestamp  AS failure_timestamp,
      'GitLab'                                    AS infra_source,
      uploaded_at,
      {{ dbt_utils.surrogate_key('base64_event', 'failure_timestamp','error_array') }} 
                                                  AS bad_event_surrogate
    FROM base

)

SELECT *
FROM renamed
ORDER BY failure_timestamp