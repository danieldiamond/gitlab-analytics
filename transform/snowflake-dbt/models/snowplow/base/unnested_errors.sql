{{
  config(
    materialized='incremental',
    sql_where='TRUE',
    unique_key='event_id'
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
FROM RAW.SNOWPLOW.EVENTS
{% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
{% endif %}