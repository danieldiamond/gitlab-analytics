{{ config({
    "schema": "analytics"
    })
}}

WITH source as (

  SELECT *
  FROM {{ source('greenhouse', 'interviewers') }}

), renamed as (

  SELECT distinct user AS interviewer_name
  FROM source

)

SELECT *
FROM renamed
