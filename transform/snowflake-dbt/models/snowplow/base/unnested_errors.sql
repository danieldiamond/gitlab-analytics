{{
  config(
    materialized='incremental'
  )
}}

SELECT
  JSONTEXT['arrivalTimestamp']::string AS arrivalTimestamp,
  JSONTEXT['attemptEndingTimestamp']::string AS attemptEndingTimestamp,
  JSONTEXT['attemptsMade']::string AS attemptsMade,
  JSONTEXT['errorCode']::string AS errorCode,
  JSONTEXT['errorMessage']::string AS errorMessage,
  JSONTEXT['lambdaArn']::string AS lambdaArn,
  JSONTEXT['rawData'::string] AS rawData,
  uploaded_at
FROM {{ var("database") }}.SNOWPLOW.EVENTS
{% if is_incremental() %}
WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
{% endif %}