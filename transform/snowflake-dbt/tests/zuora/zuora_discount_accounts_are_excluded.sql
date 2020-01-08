SELECT DISTINCT 
  rate_plan_charge.account_id
FROM {{ref('zuora_rate_plan')}} AS rate_plan
  INNER JOIN {{ref('zuora_rate_plan_charge')}} AS rate_plan_charge 
    ON rate_plan.rate_plan_id = rate_plan_charge.rate_plan_id
WHERE rate_plan.rate_plan_name = 'Discount2(%)'
