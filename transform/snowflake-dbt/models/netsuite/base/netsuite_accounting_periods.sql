WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'accounting_periods') }}

), renamed AS (

    SELECT
      {{ dbt_utils.surrogate_key('accounting_period_id', 'full_name') }}
                                                    AS accounting_period_unique_id,
      --Primary Key
      accounting_period_id::FLOAT                   AS accounting_period_id,

      --Foreign Keys
      parent_id::FLOAT                              AS parent_id,
      year_id::FLOAT                                AS year_id,

      --Info
      name::VARCHAR                                 AS accounting_period_name,
      full_name::VARCHAR                            AS accounting_period_full_name,
      fiscal_calendar_id::FLOAT                     AS fiscal_calendar_id,
      closed_on::TIMESTAMP_TZ                       AS accounting_period_close_date,
      ending::TIMESTAMP_TZ                          AS accounting_period_end_date,
      starting::TIMESTAMP_TZ                        AS accounting_period_starting_date,

      --Meta
      locked_accounts_payable::BOOLEAN              AS is_accounts_payable_locked,
      locked_accounts_receivable::BOOLEAN           AS is_accounts_receivables_locked,
      locked_all::BOOLEAN                           AS is_all_locked,
      locked_payroll::BOOLEAN                       AS is_payroll_locked,
      closed::BOOLEAN                               AS is_accouting_period_closed,
      closed_accounts_payable::BOOLEAN              AS is_accounts_payable_closed,
      closed_accounts_receivable::BOOLEAN           AS is_accounts_receivables_closed,
      closed_all::BOOLEAN                           AS is_all_closed,
      closed_payroll::BOOLEAN                       AS is_payroll_closed,
      isinactive::BOOLEAN                           AS is_accounting_period_inactive,
      is_adjustment::BOOLEAN                        AS is_accounting_period_adjustment,
      quarter::BOOLEAN                              AS is_quarter,
      year_0::BOOLEAN                               AS is_year

    FROM source

)

SELECT *
FROM renamed
