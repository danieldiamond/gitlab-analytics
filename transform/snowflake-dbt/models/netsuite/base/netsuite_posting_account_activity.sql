WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'posting_account_activity') }}

), renamed AS (

    SELECT --Foreign Keys
           accounting_period_id::FLOAT  AS accounting_period_id,
           account_id::FLOAT            AS account_id,
           department_id::FLOAT         AS department_id,
           subsidiary_id::FLOAT         AS subsidiary_id,

           --Info
           amount::FLOAT                AS actual_amount
    FROM source
    WHERE _fivetran_deleted = 'False'

)

SELECT *
FROM renamed
