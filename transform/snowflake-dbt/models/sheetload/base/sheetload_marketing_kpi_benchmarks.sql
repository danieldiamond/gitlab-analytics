{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'marketing_kpi_benchmarks') }}

), renamed AS (

    SELECT
       "Goal_Date"::DATE       AS goal_date,
       "MQL_Goal"::FLOAT       AS mql_goal,
       "NetNewOpp_Goal"::FLOAT AS net_new_opp_goal
    FROM source

)

SELECT *
FROM renamed
