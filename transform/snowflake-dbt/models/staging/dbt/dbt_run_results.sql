WITH source AS (

    SELECT *
    FROM {{ ref('dbt_run_results_source') }}

)

SELECT *
FROM source
