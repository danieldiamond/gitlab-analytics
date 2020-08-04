WITH source AS (

    SELECT *
    FROM {{ ref('dbt_test_results_source') }}

)

SELECT *
FROM source
