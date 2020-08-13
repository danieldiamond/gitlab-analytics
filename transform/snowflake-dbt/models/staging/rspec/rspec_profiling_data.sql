WITH source AS (

    SELECT *
    FROM {{ ref('rspec_profiling_data_source') }}

)


SELECT *
FROM source

