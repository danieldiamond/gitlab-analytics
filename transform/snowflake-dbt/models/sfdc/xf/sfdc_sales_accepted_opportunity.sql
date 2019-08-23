WITH opportunity AS (

  SELECT * FROM {{ref('sfdc_opportunity_xf')}}
  WHERE stage_name NOT IN ('00-Pre Opportunity','9-Unqualified','10-Duplicate')
  AND total_contract_value >= 0
  AND is_deleted = FALSE
  AND lead_source != 'Web Direct'

), account AS (

  SELECT * FROM {{ref('sfdc_account')}} 

), mid_market AS (

  SELECT 'Mid-Market'::varchar AS opportunity_segment, 
       account.account_name,
       opportunity.opportunity_name,
       opportunity.sales_accepted_date,
       opportunity.generated_source,
       opportunity.sales_segment,
       opportunity.parent_segment,
       opportunity.stage_name,
       opportunity.lead_source,
       opportunity.sales_type,
       opportunity.total_contract_value,
       opportunity.incremental_acv
  FROM opportunity
    LEFT JOIN account
    ON account.account_id = opportunity.account_id
  WHERE (opportunity.sales_type IN ('New Business','Add-On Business') 
              OR opportunity.sales_type IS NULL)
  AND   (opportunity.sales_segment = 'Mid-Market' 
              OR opportunity.parent_segment = 'Mid-Market')
  AND   opportunity.sales_segment NOT IN ('Large','Strategic')
  AND   (opportunity.parent_segment NOT IN ('Large','Strategic') 
              OR opportunity.parent_segment IS NULL)

), large_strategic as (

  SELECT CASE WHEN (opportunity.sales_segment = 'Large' 
              OR opportunity.parent_segment = 'Large') THEN 'Large'::varchar
          ELSE 'Strategic'::varchar
               END AS opportunity_segment,
         account.account_name,
         opportunity.opportunity_name,
         opportunity.sales_accepted_date,
         opportunity.generated_source,
         opportunity.sales_segment,
         opportunity.parent_segment,
         opportunity.stage_name,
         opportunity.lead_source,
         opportunity.sales_type,
         opportunity.total_contract_value,
         opportunity.incremental_acv
  FROM opportunity 
    LEFT JOIN account  
        ON account.account_id = opportunity.account_id
  WHERE (opportunity.sales_type IN ('New Business','Add-On Business') 
              OR opportunity.sales_type IS NULL)
  AND   (opportunity.sales_segment IN ('Large','Strategic') 
              OR opportunity.parent_segment IN ('Large','Strategic'))

), unioned as (

  SELECT * FROM mid_market
  UNION ALL 
  SELECT * FROM large_strategic

)

SELECT *
FROM unioned
ORDER BY sales_accepted_date