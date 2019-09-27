with source AS (

    SELECT *
    FROM {{ source('netsuite', 'accounting_periods') }}

), renamed AS (

    SELECT {{ dbt_utils.surrogate_key('accounting_period_id', 'full_name') }} AS accounting_period_unique_id,
           accounting_period_id::float                   AS accounting_period_id,
           name::varchar                                 AS account_period_name,
           full_name::varchar                            AS accounting_period_full_name,
           --keys
           fiscal_calendar_id::float                     AS fiscal_calendar_id,
           parent_id::float                              AS parent_id,
           year_id::float                                AS year_id,

           -- dates
           closed_on::timestamp_tz                       AS accounting_period_close_date,
           ending::timestamp_tz                          AS accounting_period_end_date,
           starting::timestamp_tz                        AS accounting_period_starting_date,
           -- info
           locked_accounts_payable::boolean              AS is_accounts_payable_locked,
           locked_accounts_receivable::boolean           AS is_accounts_receivables_locked,
           locked_all::boolean                           AS is_all_locked,
           locked_payroll::boolean                       AS is_payroll_locked,

           closed::boolean                               AS is_accouting_period_closed,
           closed_accounts_payable::boolean              AS is_accounts_payable_closed,
           closed_accounts_receivable::boolean           AS is_accounts_receivables_closed,
           closed_all::boolean                           AS is_all_closed,
           closed_payroll::boolean                       AS is_payroll_closed,

           isinactive::boolean                           AS is_accounting_period_inactive,
           is_adjustment::boolean                        AS is_accounting_period_adjustment,

           quarter::boolean                              AS is_quarter,
           year_0::boolean                               AS is_year

    FROM source

)

SELECT *
FROM renamed
