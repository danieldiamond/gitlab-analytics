WITH zuora_base_mrr AS (

    SELECT * 
    FROM  {{ ref('zuora_base_mrr') }} 

)

, zuora_account AS (

    SELECT * 
    FROM {{ ref('zuora_account')}}
)

, charges AS (

    SELECT * 
    FROM {{ ref('customers_db_charges_xf')}}
)

, namespaces AS (

    SELECT * 
    FROM {{ ref('gitlab_dotcom_namespaces_xf')}}
)

, plans AS (

    SELECT * 
    FROM {{ ref('gitlab_dotcom_plans')}}
)

, zuora_account_to_sfdc_account_helper AS (

    SELECT * 
    FROM {{ ref('zuora_account_to_sfdc_account_helper')}}
)

, data AS (

    SELECT 
      zuora_base_mrr.account_name,
      zuora_base_mrr.account_number,
      zuora_base_mrr.subscription_name_slugify,
      zuora_account.crm_id,
      current_customer_id,
      current_gitlab_namespace_id,
      zuora_base_mrr.product_category,
      namespace_id,
      namespaces.plan_id,
      plans.plan_name,
      lower(zuora_base_mrr.product_category) = plans.plan_name AS valid
    FROM zuora_base_mrr
    LEFT JOIN zuora_account
      ON zuora_base_mrr.account_number  = zuora_account.account_number 
    LEFT JOIN charges
      ON zuora_base_mrr.subscription_name_slugify = charges.subscription_name_slugify
        AND zuora_base_mrr.rate_plan_charge_id = charges.rate_plan_charge_id
    LEFT JOIN namespaces 
      ON charges.current_gitlab_namespace_id = namespaces.namespace_id
    LEFT JOIN plans
      ON namespaces.plan_id = plans.plan_id::VARCHAR
    LEFT JOIN zuora_account_to_sfdc_account_helper
      ON zuora_account.account_id = zuora_account_to_sfdc_account_helper.zuora_account_id

  WHERE TRUE
    AND zuora_base_mrr.delivery = 'SaaS'
  
)

SELECT *
FROM data
