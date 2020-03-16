WITH zuora_invoice AS (
    SELECT *
    FROM {{ ref('zuora_invoice_source') }}

), zuora_invoice_item AS (

SELECT *
FROM {{ ref('zuora_invoice_item_source') }}

    ), invoice_data AS (
select --zuora_invoice.INVOICE_ID,
    zuora_invoice_item.invoice_item_id,
    zuora_invoice_item.RATE_PLAN_CHARGE_ID,
    zuora_invoice.ACCOUNT_ID,
    zuora_invoice_item.product_id,
    zuora_invoice_item.subscription_id as item_subscription_id,
    zuora_invoice.DUE_DATE::DATE AS invoice_due_date,
    zuora_invoice.INVOICE_NUMBER,
    zuora_invoice.INVOICE_DATE::DATE AS invoice_date,
    zuora_invoice.STATUS as invoice_status,
    zuora_invoice.POSTED_DATE::DATE as posted_date,
    --zuora_invoice.amount as invoice_amount,
    --zuora_invoice.amount_without_tax,
    zuora_invoice_item.SKU,
    zuora_invoice_item.service_start_Date::DATE as service_start_Date,
    zuora_invoice_item.SERVICE_END_DATE::DATE as SERVICE_END_DATE,
    date_trunc('month', zuora_invoice_item.service_start_date)::DATE AS service_start_month,
    date_trunc('month', zuora_invoice_item.SERVICE_END_DATE)::DATE   AS service_end_month,
    zuora_invoice_item.CHARGE_AMOUNT,
    zuora_invoice_item.tax_amount,
    zuora_invoice_item.CHARGE_DATE::DATE as charge_date,
    zuora_invoice_item.CHARGE_NAME,
    LOWER(zuora_invoice_item.charge_name) LIKE '%trueup%' AS is_trueup
from zuora_invoice_item
    join zuora_invoice
ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
    where zuora_invoice.is_deleted = FALSE and zuora_invoice_item.is_deleted=FALSE
    )

select *
from invoice_data