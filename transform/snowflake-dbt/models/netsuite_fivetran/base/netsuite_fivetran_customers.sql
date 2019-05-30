WITH source AS (

    SELECT *
    FROM {{ source('netsuite_fivetran', 'customers') }}

), renamed AS (

    SELECT customer_id,
           companyname  AS customer_name,
           name         AS customer_alt_name,
           full_name    AS customer_full_name,
           -- keys
           subsidiary_id,
           currency_id,
           department_id,
           parent_id,
           rev_rec_forecast_rule_id,

           --deposit_balance_foreign
           openbalance  AS customer_balance,
           days_overdue AS days_overdue

    FROM source
    WHERE _fivetran_deleted = 'False'
)

SELECT *
FROM renamed
