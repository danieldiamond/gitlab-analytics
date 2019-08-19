{{ config({
    "schema": "staging"
    })
}}

WITH renamed AS (
  SELECT DISTINCT
    id                                     AS order_id,
    customer_id,
    product_rate_plan_id,
    subscription_id,
    subscription_name,
    {{zuora_slugify("subscription_name")}} AS subscription_name_slugify,
    start_date                             AS order_start_date,
    end_date                               AS order_end_date,
    quantity                               AS order_quantity,
    created_at                             AS order_created_at,
    updated_at                             AS order_updated_at,
    DENSE_RANK() OVER (
        PARTITION BY order_id
        ORDER BY order_updated_at DESC) AS row_number
    --gl_namespace_id,
    --gl_namespace_name,
    --amendment_type,
    --trial,
    --last_extra_ci_minutes_sync_at,
 FROM {{ source('customers', 'customers_db_orders') }}
)

SELECT
  *
FROM renamed
WHERE row_number = 1
