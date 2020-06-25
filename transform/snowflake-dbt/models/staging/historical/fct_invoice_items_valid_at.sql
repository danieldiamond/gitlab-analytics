WITH zuora_invoice_item AS (

    SELECT *
    FROM {{ ref('zuora_invoice_item_snapshots_source') }}

), zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_snapshots_source') }}
    WHERE '{{ var('valid_at') }}'::TIMESTAMP >= dbt_valid_from
     AND '{{ var('valid_at') }}'::TIMESTAMP < coalesce_to_infinity(dbt_valid_to)


), invoice_charges AS (

    SELECT
      zuora_invoice_item.invoice_item_id            AS invoice_item_id,
      zuora_invoice.invoice_id                      AS invoice_id,
      zuora_invoice_item.rate_plan_charge_id        AS charge_id,
      zuora_invoice.invoice_number                  AS invoice_number,
      zuora_invoice.account_id                      AS invoice_account_id,
      zuora_invoice.invoice_date::DATE              AS invoice_date,
      zuora_invoice_item.service_start_date::DATE   AS service_start_date,
      zuora_invoice_item.service_end_date::DATE     AS service_end_date,
      zuora_invoice.amount_without_tax              AS invoice_amount_without_tax,
      zuora_invoice_item.charge_amount              AS invoice_item_charge_amount,
      zuora_invoice_item.unit_price                 AS invoice_item_unit_price
    FROM zuora_invoice_item
    INNER JOIN zuora_invoice
      ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
    WHERE zuora_invoice.status = 'Posted'
    AND zuora_invoice.is_deleted = FALSE
    AND zuora_invoice_item.is_deleted = FALSE

)

SELECT *
FROM invoice_charges