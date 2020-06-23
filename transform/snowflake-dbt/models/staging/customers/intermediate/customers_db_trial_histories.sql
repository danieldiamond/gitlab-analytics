WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_trial_histories_source') }}

)

SELECT *
FROM source