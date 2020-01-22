WITH invoices AS (

    SELECT * FROM {{ref('zuora_invoice')}}

), zuora_account AS (

	SELECT * FROM {{ref('zuora_account')}}

/* Self join invoices before and after the invoice date to get a sum of other transactions paid. */
), joined AS (
    SELECT DISTINCT
      invoices.invoice_id,
      invoices.amount                                  AS refund_amount,
      invoices.account_id,
      invoices.invoice_date                            AS refund_date,
      DATE_TRUNC('month', invoices.invoice_date)::DATE AS refund_month,
      zuora_account.crm_id,
      zuora_account.sfdc_entity,
      zuora_account.account_name,
      zuora_account.account_number,
      zuora_account.currency,
      SUM(COALESCE(before_and_after.amount, 0))        AS before_and_after_amount_sum
    FROM invoices
      LEFT JOIN zuora_account
        ON invoices.account_id = zuora_account.account_id
      LEFT JOIN invoices AS before_and_after
        ON invoices.account_id = before_and_after.account_id
        AND before_and_after.invoice_date BETWEEN DATEADD('days', -60, invoices.invoice_date) AND DATEADD('days', 60, invoices.invoice_date)
    {{ dbt_utils.group_by(10) }}
)

SELECT
  joined.*
FROM joined
WHERE amount < 0 -- negative amount = refund
  AND before_and_after_amount_sum <= 0 -- customer ended up even ($0) or better (<$0)
ORDER BY amount
