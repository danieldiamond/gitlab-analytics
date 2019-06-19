WITH deleted AS (

    SELECT *
    FROM {{ ref('netsuite_stitch_deleted') }}

), transactions AS (

    SELECT netsuite_stitch_transaction.*
    FROM {{ ref('netsuite_stitch_transaction') }}
    LEFT JOIN deleted on deleted.internal_id = netsuite_stitch_transaction.transaction_id
    WHERE deleted.deleted_date IS NULL

), accounting_periods AS (

    SELECT *
    FROM {{ ref('netsuite_stitch_accounting_periods') }}

)

SELECT
  {{ dbt_utils.star(from=ref('netsuite_stitch_transaction'), except=["POSTING_PERIOD_NAME", "POSTING_PERIOD_ID", "PERIOD_DATE"]) }},
  COALESCE(transactions.posting_period_name, accounting_periods.period_name) as posting_period_name,
  COALESCE(transactions.posting_period_id,accounting_periods.accounting_period_id) as posting_period_id,
  COALESCE(to_date(transactions.posting_period_name, 'Mon YYYY'), to_date(accounting_periods.period_name, 'Mon YYYY')) as period_date
FROM transactions
LEFT JOIN accounting_periods
    ON date_trunc('month', accounting_periods.start_date)::DATE = date_trunc('month', transaction_date)::DATE
WHERE accounting_periods.is_quarter = FALSE AND accounting_periods.is_year = FALSE
