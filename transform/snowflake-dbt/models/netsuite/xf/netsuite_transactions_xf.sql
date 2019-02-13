WITH deleted AS (

    SELECT *
    FROM {{ ref('netsuite_deleted') }}

), transactions AS (

    SELECT t.*
    FROM {{ ref('netsuite_transactions') }} t
    LEFT JOIN deleted on deleted.internal_id = t.transaction_id
    WHERE deleted.deleted_timestamp IS NULL

), accounting_periods AS (

    SELECT *
    FROM {{ ref('netsuite_accounting_periods') }}

)

SELECT
  {{ dbt_utils.star(from=ref('netsuite_transactions'), except=["POSTING_PERIOD_NAME", "POSTING_PERIOD_ID", "PERIOD_DATE"]) }} ,
  coalesce(t.POSTING_PERIOD_NAME, ap.PERIOD_DATE_NAME) as posting_period_name,
  coalesce(t.POSTING_PERIOD_ID,ap.POSTING_PERIOD_ID) as posting_period_id,
  coalesce(t.PERIOD_DATE, to_date(ap.PERIOD_DATE_NAME, 'Mon YYYY')) as period_date
FROM transactions t
  LEFT JOIN accounting_periods ap ON date_trunc('month', ap.FIRST_DAY_OF_MONTH) :: DATE = date_trunc('month', TRANSACTION_DATE) :: DATE
WHERE ap.IS_QUARTER = FALSE AND ap.IS_YEAR = FALSE