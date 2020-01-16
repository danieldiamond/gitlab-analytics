WITH invoices AS (

    SELECT * FROM {{ref('zuora_invoice')}}

), zuora_account AS (

	SELECT * FROM {{ref('zuora_account')}}

/* Self join invoices before and after the invoice date to get a sum of other transactions paid. */
), joined AS (
    SELECT DISTINCT
      base.*,
      DATE_TRUNC('month', base.invoice_date)::DATE AS refund_month,
      za.crm_id,
      za.sfdc_entity,
      za.account_name,
      za.account_number,
      za.currency
      SUM(COALESCE(before_and_after.amount, 0)) OVER (
        PARTITION BY zuora_account.crm_id
        ORDER BY base.invoice_date
      ) AS before_and_after_amount_sum
    FROM invoices AS base
      LEFT JOIN zuora_account
        ON base.account_id = zuora_account.account_id
      LEFT JOIN invoices AS before_and_after
        ON base.account_id = before_and_after.account_id
        AND before_and_after.invoice_date BETWEEN DATEADD('days', -60, base.invoice_date) AND DATEADD('days', 60, base.invoice_date)
)

SELECT
  joined.*
FROM joined
WHERE amount < 0 -- negative amount = refund
  AND before_and_after_amount_sum <= 0 -- customer ended up even ($0) or better (<$0)
ORDER BY amount
