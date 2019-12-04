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

WITH date_spine AS (

    SELECT DISTINCT
      DATE_TRUNC('day', date_day) AS date_actual
    FROM {{ref("date_details")}}
    WHERE date_day >= '2019-02-01'::DATE
      AND date_day <= '2019-10-01'::DATE 

), first_snapshot AS (

    SELECT
      id                 AS opportunity_id,
      valid_to,
      {% for field in fields_to_use %}
      {{field}}::VARCHAR AS {{field}},
      {% endfor %}
      createddate        AS created_at,
      valid_from
    FROM {{ref('sfdc_opportunity_snapshots_base')}}  
    WHERE date_actual = '2019-10-01'::DATE

), base AS (

    SELECT
      field_history.opportunity_id,
      field_modified_at                AS valid_to,
      opportunity_field,
      COALESCE(old_value, 'true null') AS old_value
    FROM {{ref('sfdc_opportunity_field_history')}} field_history
    INNER JOIN first_snapshot
      ON field_history.field_modified_at <= first_snapshot.valid_from
     AND field_history.opportunity_id = first_snapshot.opportunity_id 
    WHERE opportunity_field IN ('{{ fields_to_use | join ("', '") }}')

), base_pivoted AS (

    SELECT *
    FROM base
      PIVOT(MAX(old_value) FOR opportunity_field IN ('{{ fields_to_use | join ("', '") }}'))
      
), unioned AS (

    SELECT *
    FROM first_snapshot 
    UNION
    SELECT 
      base_pivoted.*,
      NULL::TIMESTAMP_TZ AS created_at,
      NULL::TIMESTAMP_TZ AS valid_from
    FROM base_pivoted
 
), filled AS (

    SELECT
      opportunity_id,
      {% for field in fields_to_use %}
      FIRST_VALUE({{field}}) IGNORE NULLS 
        OVER (PARTITION BY opportunity_id ORDER BY valid_to ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS {{field}},
      {% endfor %}      
      FIRST_VALUE(created_at) IGNORE NULLS 
        OVER (PARTITION BY opportunity_id ORDER BY valid_to)                                                  AS created_date,
      COALESCE(IFNULL(valid_from, LAG(valid_to) 
        OVER (PARTITION BY opportunity_id ORDER BY valid_to)), created_date)                                  AS valid_from,
      valid_to
    FROM unioned

)

SELECT *
FROM filled
QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id, DATE_TRUNC('day', valid_from) ORDER BY valid_from DESC) = 1
ORDER BY opportunity_id, valid_from