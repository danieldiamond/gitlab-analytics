/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{ config(materialized='table',
  transient=false)}}

WITH charges_agg AS (

    SELECT *
    FROM {{ ref('charges_agg') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), last_month_of_fiscal_quarter AS (

    SELECT DISTINCT
      DATE_TRUNC('month', last_day_of_fiscal_quarter) AS last_month_of_fiscal_quarter,
      fiscal_quarter_name_fy
    FROM {{ ref('dim_dates') }}

), last_month_of_fiscal_year AS (

    SELECT DISTINCT
      DATE_TRUNC('month', last_day_of_fiscal_year) AS last_month_of_fiscal_year,
      fiscal_year
    FROM {{ ref('dim_dates') }}

), charges_month_by_month AS (

    SELECT
      charges_agg.*,
      dim_dates.date_actual  AS arr_month
    FROM charges_agg
    INNER JOIN dim_dates
      ON charges_agg.effective_start_month <= dim_dates.date_actual
      AND (charges_agg.effective_end_month > dim_dates.date_actual OR charges_agg.effective_end_month IS NULL)
      AND dim_dates.day_of_month = 1
    WHERE subscription_status NOT IN ('Draft', 'Expired')
      AND charges_agg.charge_type = 'Recurring'
      AND mrr != 0

)

    SELECT
      --primary_key
      {{ dbt_utils.surrogate_key(['arr_month', 'subscription_name', 'product_category']) }}
                                      AS primary_key,

      --date info
      arr_month,
      quarter.fiscal_quarter_name_fy,
      year.fiscal_year,
      subscription_start_month,
      subscription_end_month,

      --account info
      zuora_account_id,
      zuora_sold_to_country,
      zuora_account_name,
      zuora_account_number,
      crm_id,
      ultimate_parent_account_id,
      ultimate_parent_account_name,
      ultimate_parent_billing_country,
      ultimate_parent_account_segment,
      ultimate_parent_industry,
      ultimate_parent_account_owner_team,
      ultimate_parent_territory,

      --subscription info
      subscription_name,
      subscription_status,

      --charge info
      product_category,
      delivery,
      service_type,
      charge_type,
      array_agg(unit_of_measure)    AS unit_of_measure,
      array_agg(rate_plan_name)     AS rate_plan_name,
      SUM(mrr)                      AS mrr,
      SUM(arr)                      AS arr,
      SUM(quantity)                 AS quantity
    FROM charges_month_by_month
    LEFT JOIN last_month_of_fiscal_quarter quarter
      ON charges_month_by_month.arr_month = quarter.last_month_of_fiscal_quarter
    LEFT JOIN last_month_of_fiscal_year year
      ON  charges_month_by_month.arr_month = year.last_month_of_fiscal_year
    {{ dbt_utils.group_by(n=24) }}
