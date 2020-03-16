WITH dim_product AS (
    SELECT *
    FROM {{ ref('dim_product') }}

), dim_account AS (

SELECT *
FROM {{ ref('dim_account') }}

),  dim_subscription AS (

SELECT *
FROM {{ ref('dim_subscription') }}
),  fct_invoice_item AS (

SELECT *
FROM {{ ref('fct_invoice_item') }}
),  fct_rate_plan_charge AS (

SELECT *
FROM {{ ref('fct_rate_plan_charges') }}
),
 date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1

),olap_cube as (
    select *
    from fct_invoice_item
    inner join fct_rate_plan_charges using(rate_plan_charge_it)
    inner join dim_account using(account_id)
    inner join dim_product using(product_id)
    inner join dim_subscription on dim_subscription.subscription_id = fct_invoice_item.item_subscription_id
    WHERE subscription_status NOT IN ('Draft','Expired')
    ),


