WITH fct_charges AS (

    SELECT *
    FROM {{ ref('fct_charges_valid_at') }}

), dim_accounts AS (

    SELECT *
    FROM {{ ref('dim_accounts_valid_at') }}

), dim_dates AS (

   SELECT *
   FROM {{ ref('dim_dates') }}
   WHERE date_day = '{{ var('valid_at') }}'

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions_valid_at') }}


), dim_products AS (

    SELECT *
    FROM {{ ref('dim_products_valid_at') }}

), charges_month_by_month AS (

   SELECT
    fct_charges.*,
    dim_dates.date_id,
    dateadd('month', -1, dim_dates.date_actual)  AS reporting_month
    FROM fct_charges
    INNER JOIN fct_invoice_items_agg
      ON fct_charges.charge_id = fct_invoice_items_agg.charge_id
    INNER JOIN dim_dates
      ON fct_charges.effective_start_date <= '{{ var('valid_at') }}'
        AND (fct_charges.effective_end_date > '{{ var('valid_at') }}' OR fct_charges.effective_end_date IS NULL)
        AND dim_dates.day_of_month=1
)

SELECT
  '{{ var('valid_at') }}' AS snapshot_date,
  dim_accounts.account_number,
  dim_accounts.account_name,
  dim_subscriptions.subscription_name,
  sum(charges_month_by_month*12)  AS arr

  FROM charges_month_by_month
  INNER JOIN dim_subscriptions
    ON dim_subscriptions.subscription_id = charges_month_by_month.subscription_id
  INNER JOIN dim_products
    ON charges_month_by_month.product_id = dim_products.product_id
  INNER JOIN dim_accounts
    ON charges_month_by_month.account_id = dim_accounts.account_id
  WHERE subscription_status NOT IN ('Draft','Expired')
  GROUP BY 1,2,3,4

