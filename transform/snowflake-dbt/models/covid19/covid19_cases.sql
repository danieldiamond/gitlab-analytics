{{
    config({
        "schema": "covid19"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('covid19', 'cases') }}

), renamed AS (

    SELECT 
      country_region::VARCHAR,
      province_state::VARCHAR,
      date::DATE,
      case_type::VARCHAR,
      cases::NUMBER             AS case_count,
      long::FLOAT               AS longitude,
      lat::FLOAT                AS latitude,
      difference::NUMBER        AS case_count_change,
      last_updated_date::TIMESTAMP
    FROM source

)

SELECT *
FROM renamed
