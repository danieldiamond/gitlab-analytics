WITH valid_charges AS (
  
    SELECT * 
    FROM {{ ref('customers_db_valid_charges') }}
  
)

, charges_for_expired_transactions AS (
  
    SELECT * 
    FROM {{ ref('customers_db_charges_for_expired_transactions') }}
  
)

, zuora_base_mrr AS (
  
    SELECT * 
    FROM {{ ref('zuora_base_mrr') }}
  
)

, filtered_out_charges_for_expired_transactions AS (
  
    SELECT charges_for_expired_transactions.*
    FROM charges_for_expired_transactions
    LEFT JOIN valid_charges ON charges_for_expired_transactions.rate_plan_charge_id = valid_charges.rate_plan_charge_id 
    WHERE valid_charges.rate_plan_charge_id IS NULL

)

, unioned_charges AS (
  
  SELECT *
  FROM valid_charges
  
  UNION 
  
  SELECT *
  FROM filtered_out_charges_for_expired_transactions
  
)

, joined_with_base_mrr AS (
  
    SELECT
      unioned_charges.rate_plan_charge_id,
      unioned_charges.subscription_name_slugify,
      rate_plan_id,
      is_purchased_through_subscription_portal,
      current_customer_id,
      current_gitlab_namespace_id,
      first_customer_id,
      is_started_with_trial,
      trial_start_date,
      
      -- Subscription metadata
      zuora_base_mrr.lineage,
      zuora_base_mrr.oldest_subscription_in_cohort,
      zuora_base_mrr.subscription_start_date,
      zuora_base_mrr.subscription_status,
      
      zuora_base_mrr.effective_start_date,
      zuora_base_mrr.effective_end_date,
      
      -- Product Category Info
      zuora_base_mrr.delivery,
      zuora_base_mrr.product_category,
      
      -- Financial Info
      zuora_base_mrr.mrr,
      zuora_base_mrr.tcv
    FROM unioned_charges
    LEFT JOIN zuora_base_mrr
      ON unioned_charges.rate_plan_charge_id = zuora_base_mrr.rate_plan_charge_id

)

SELECT * 
FROM joined_with_base_mrr
