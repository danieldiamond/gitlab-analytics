WITH zuora_invoice AS (
    SELECT *
    FROM {{ ref('zuora_invoice_source') }}

), zuora_invoice_item AS (

SELECT *
FROM {{ ref('zuora_invoice_item_source') }}

    ), invoice_data AS (
select
    zuora_invoice_item.RATE_PLAN_CHARGE_ID as charge_id,
    zuora_invoice_item.SKU as SKU,
    MIN(zuora_invoice_item.service_start_Date::DATE) as service_start_Date,
    MAX(zuora_invoice_item.SERVICE_END_DATE::DATE) as SERVICE_END_DATE,
    MIN(date_trunc('month', zuora_invoice_item.service_start_date)::DATE) AS service_start_month,
    MAX(date_trunc('month', zuora_invoice_item.SERVICE_END_DATE)::DATE)   AS service_end_month,
    SUM(zuora_invoice_item.CHARGE_AMOUNT) as charge_amount_sum,
    SUM(zuora_invoice_item.tax_amount) AS tax_amount_sum
from zuora_invoice_item
    join zuora_invoice
ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
    where zuora_invoice.is_deleted = FALSE and zuora_invoice_item.is_deleted=FALSE
and zuora_invoice.status='Posted'
    group by 1,2)

select *
from invoice_data