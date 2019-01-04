WITH deleted AS (

    SELECT *
    FROM {{ ref('netsuite_deleted') }}

), transactions AS (

    SELECT t.*
    FROM {{ ref('netsuite_transactions') }} t
    LEFT JOIN deleted on deleted.internal_id = t.transaction_id
    WHERE deleted.deleted_timestamp IS NULL

)

SELECT *
FROM transactions