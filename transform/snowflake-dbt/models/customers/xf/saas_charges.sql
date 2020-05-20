WITH saas_zuora_charges AS (
  
    SELECT * 
    FROM {{ ref('fct_charges') }}
    WHERE delivery = 'saas'
  
)

, customers_db_charges AS (
  
    SELECT * 
    FROM {{ ref('zuora_base_mrr') }}
  
)

, dim_accounts AS (

    SELECT *
    FROM {{ ref('dim_accounts') }}
)

, dim_customers AS (

    SELECT *
    FROM {{ ref('dim_customers') }}
)


, joined AS (

    SELECT dim_customers.*
    FROM saas_zuora_charges
    LEFT JOIN customers_db_charges 
      ON saas_zuora_charges.charge_id = customers_db_charges.rate_plan_charge_id
    LEFT JOIN dim_accounts
      ON saas_zuora_charges.account_id = dim_accounts.account_id
    LEFT JOIN dim_customers
      ON dim_accounts.crm_id = dim_customers.crm_id
    LEFT JOIN dim_customers AS merged_accounts
      ON dim_customers.merged_to_account_id = merged_accounts.crm_id

)

SELECT *
FROM joined
