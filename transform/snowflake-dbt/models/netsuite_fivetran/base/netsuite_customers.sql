WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'customers') }}

), renamed AS (

    SELECT customer_id::float                 AS customer_id,
           companyname::varchar               AS customer_name,
           name::varchar                      AS customer_alt_name,
           full_name::varchar                 AS customer_full_name,
           -- keys
           subsidiary_id::float               AS subsidiary_id,
           currency_id::float                 AS currency_id,
           department_id::float               AS department_id,
           parent_id::float                   AS parent_id,
           rev_rec_forecast_rule_id::float    AS rev_rec_forecast_rule_id,

           --deposit_balance_foreign
           openbalance::float                 AS customer_balance,
           days_overdue::float                AS days_overdue

    FROM source
    WHERE _fivetran_deleted = 'False'
)

SELECT *
FROM renamed
