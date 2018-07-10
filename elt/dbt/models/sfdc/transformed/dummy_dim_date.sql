with base as (

	SELECT * FROM {{ref('dim_date')}}

), final as (

SELECT date_actual, 
		day_of_quarter, 
		TRUE as is_won
FROM base
WHERE date_actual > CURRENT_DATE - 9
AND date_actual < CURRENT_DATE + 90

)

SELECT NULL::varchar AS opportunity_id
       , NULL::bigint AS account_id
       , NULL::bigint AS opportunity_stage_id
       , NULL::bigint AS lead_source_id
       , NULL::numeric AS weighted_iacv
       , NULL::varchar AS opportunity_type
       , NULL::text as opportunity_sales_segmentation
       , NULL::date as sales_qualified_date
       , NULL::date as sales_accepted_date
       , NULL::text as sales_qualified_source
       , NULL::date AS opportunity_closedate
       , NULL::text as opportunity_product
       , NULL::text as billing_period
       , NULL::text as opportunity_name
       , NULL::text as ownerid
       , NULL::boolean as is_risky
       , NULL::integer as days_in_stage
       , NULL::timestamp as lastactivitydate
       , NULL::text as competitors__C
       , NULL::numeric AS quantity
       , NULL::numeric as iacv
       , NULL::numeric as renewal_acv
       , NULL::numeric as acv
       , NULL::numeric as tcv
       , NULL::text as deal_size
       , is_won::boolean as is_won
       , date_actual::date as date_actual_dummy
FROM final