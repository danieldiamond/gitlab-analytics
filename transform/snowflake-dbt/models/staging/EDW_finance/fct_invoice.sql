WITH zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice') }}

), zuora_invoice_item AS (

    SELECT *
    FROM {{ ref('zuora_invoice_item') }}

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}
    WHERE is_deleted = FALSE

)
SELECT
      zuora_invoice.invoice_number,
      zuora_invoice_item.invoice_item_id,
      zuora_invoice_item.rate_plan_charge_id,
      zuora_invoice.invoice_date::DATE              AS invoice_date,
      zuora_invoice_item.service_start_date::DATE   AS service_start_date,
      zuora_invoice_item.service_end_date::DATE     AS service_end_date,
      --zuora_invoice.amount_without_tax              AS invoice_amount_without_tax,
      zuora_invoice_item.charge_amount              AS invoice_item_charge_amount,
      zuora_invoice_item.tax_amount                 AS invoice_item_tax_amount,
      zuora_invoice.status                          AS invoice_status
    FROM zuora_invoice_item
    INNER JOIN zuora_invoice
      ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
