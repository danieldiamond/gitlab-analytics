WITH deleted AS (

    SELECT *
    FROM {{ ref('netsuite_stitch_deleted') }}

), transactions AS (

    SELECT t.*
    FROM {{ ref('netsuite_stitch_transaction') }} t
    LEFT JOIN deleted on deleted.internal_id = t.transaction_id
    WHERE deleted.deleted_date IS NULL

), accounting_periods AS (

    SELECT *
    FROM {{ ref('netsuite_stitch_accounting_periods') }}

)

SELECT
  {{ dbt_utils.star(from=ref('netsuite_stitch_transaction'), except=["POSTING_PERIOD_NAME", "POSTING_PERIOD_ID", "PERIOD_DATE"]) }},
  COALESCE(t.posting_period_name, ap.period_name) as posting_period_name,
  COALESCE(t.posting_period_id,ap.accounting_period_id) as posting_period_id,
  COALESCE(to_date(t.posting_period_name, 'Mon YYYY'), to_date(ap.period_name, 'Mon YYYY')) as period_date
FROM transactions t
  LEFT JOIN accounting_periods ap ON date_trunc('month', ap.start_date) :: DATE = date_trunc('month', transaction_date) :: DATE
WHERE ap.is_quarter = FALSE AND ap.is_year = FALSE