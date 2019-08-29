{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'deleted') }}

), renamed AS (

    SELECT
        {{ dbt_utils.surrogate_key('internalid', '_sdc_sequence') }} AS deleted_transactions_unique_id,
        deleteddate     AS deleted_date,
        internalid      AS internal_id,
        type,
        externalid      AS external_id,
        customrecord    AS customer_record,
        name
    FROM source

)

SELECT *
FROM renamed


