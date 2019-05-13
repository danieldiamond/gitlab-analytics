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
FROM {{ source('snowplow', 'events') }}
WHERE 1 = 1
{% if is_incremental() %}
AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
{% endif %}
{% if target.name == "ci" %}
AND arrivalTimestamp > dateadd(day, -8, current_date)
{% endif  %}
