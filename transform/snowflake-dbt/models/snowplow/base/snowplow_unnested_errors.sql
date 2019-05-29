{{
  config(
    materialized='incremental'
  )
}}

WITH base as (

  SELECT *
  {% if target.name not in ("prod") -%}

  FROM {{ source('snowplow', 'events_sample') }}

  {%- else %}

  FROM {{ source('snowplow', 'events') }}

  {%- endif %}

  WHERE length(JSONTEXT['errorCode']) > 0

  {%- if is_incremental() %}
    AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
  {% endif -%}

)

SELECT
  JSONTEXT['arrivalTimestamp']::string        AS arrivalTimestamp,
  JSONTEXT['attemptEndingTimestamp']::string  AS attemptEndingTimestamp,
  JSONTEXT['attemptsMade']::string            AS attemptsMade,
  JSONTEXT['errorCode']::string               AS errorCode,
  JSONTEXT['errorMessage']::string            AS errorMessage,
  JSONTEXT['lambdaArn']::string               AS lambdaArn,
  JSONTEXT['rawData'::string]                 AS rawData,
  uploaded_at
FROM base