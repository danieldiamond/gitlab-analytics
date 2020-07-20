{{
  config( materialized='ephemeral')
}}

WITH zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_snapshots_source') }}
    WHERE '{{ var('valid_at') }}'::DATE >= dbt_valid_from
      AND '{{ var('valid_at') }}'::DATE < {{ coalesce_to_infinity('dbt_valid_to') }}

), zuora_invoice_item AS (

    SELECT *
    FROM {{ ref('zuora_invoice_item_snapshots_source') }}
    WHERE '{{ var('valid_at') }}'::DATE >= dbt_valid_from
      AND '{{ var('valid_at') }}'::DATE < {{ coalesce_to_infinity('dbt_valid_to') }}

), invoice_data AS (

  SELECT
    zuora_invoice_item.RATE_PLAN_CHARGE_ID            AS charge_id,
    zuora_invoice_item.SKU AS SKU,
    MIN(zuora_invoice_item.service_start_Date::DATE)  AS service_start_date,
    MAX(zuora_invoice_item.SERVICE_END_DATE::DATE)    AS service_end_date,
    SUM(zuora_invoice_item.CHARGE_AMOUNT)             AS charge_amount_sum,
    SUM(zuora_invoice_item.tax_amount)                AS tax_amount_sum
  FROM zuora_invoice_item
  INNER JOIN zuora_invoice
    ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
  WHERE zuora_invoice.is_deleted = FALSE
    AND zuora_invoice_item.is_deleted= FALSE
    AND zuora_invoice.status='Posted'
  GROUP BY 1, 2
)

SELECT *
FROM invoice_data