SELECT
  JSONTEXT['arrivalTimestamp']::string AS arrivalTimestamp,
  JSONTEXT['attemptEndingTimestamp']::string AS attemptEndingTimestamp,
  JSONTEXT['attemptsMade']::string AS attemptsMade,
  JSONTEXT['errorCode']::string AS errorCode,
  JSONTEXT['errorMessage']::string AS errorMessage,
  JSONTEXT['lambdaArn']::string AS lambdaArn,
  JSONTEXT['rawData'::string] AS rawData
FROM RAW.SNOWPLOW.EVENTS
