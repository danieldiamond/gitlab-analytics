{% set fields_to_use = [
    'account_owner_team_o__c','amount','business_development_rep__c','closedate',
    'expected_close_date__c','forecastcategoryname','incremental_acv_2__c',
    'leadsource','next_steps__c','opportunity_term_new__c','renewal_acv__c',
    'renewal_amount__c','renewal_forecast_category__c','sales_accepted_date__c',
    'sales_qualified__c','sales_qualified_date__c','sales_segmentation_employees_o__c',
    'sales_segmentation_o__c','sql_amount__c','sql_source__c','stagename','swing_deal__c',
    'type','ultimate_parent_sales_segment_emp_o__c','ultimate_parent_sales_segment_o__c',
    'upside_iacv__c'
] %}

WITH base AS (

    SELECT
      opportunity_id,
      field_modified_at                AS valid_to,
      LOWER(opportunity_field)         AS opportunity_field,
      COALESCE(old_value, 'true null') AS old_value
    FROM {{ref('sfdc_opportunity_field_history')}}
    WHERE opportunity_field NOT IN ('owner', 'created', 'locked', 'unlocked', 'opportunitycreatedfromlead')

), base_pivoted AS (

    SELECT *
    FROM base
      PIVOT(MAX(old_value) FOR opportunity_field IN {{ fields_to_use }})
      
), first_snapshot AS (

    SELECT
      id          AS opportunity_id,
      valid_to,
      {% for field in fields_to_use %}
      {{field}}::VARCHAR AS {{field}},
      {% endfor %}
      createddate AS created_date,
      valid_from
    FROM {{ref('sfdc_opportunity_snapshots_base')}}  
    WHERE date_actual = '2019-10-01'::DATE

), unioned AS (

    SELECT 
      base_pivot.*,
      NULL::TIMESTAMP_TZ AS created_date,
      NULL::TIMESTAMP_TZ AS valid_from
    FROM base_pivot
    UNION
    SELECT *
    FROM first_snapshot  

)

SELECT *
FROM unioned