with source AS (

    SELECT *
    FROM {{ source('netsuite_fivetran', 'accounts') }}

), renamed AS (

    SELECT  account_id,
            name                                    AS account_name,
            full_name                               AS account_full_name,
            full_description                        AS account_full_description,

            -- keys
            accountnumber                           AS account_number,
            currency_id,
            department_id,
            expense_type_id,
            parent_id                               AS parent_account_id,
            -- info
            type_name                               AS account_type,
            type_sequence                           AS account_type_sequence,
            openbalance                             AS current_account_balance,

            cashflow_rate_type,
            general_rate_type,

            isinactive::boolean                     AS is_account_inactive,
            is_balancesheet::boolean                AS is_balancesheet_account,
            is_included_in_elimination::boolean     AS is_account_included_in_elimination,
            is_included_in_reval::boolean           AS is_account_included_in_reval,
            is_including_child_subs::boolean        AS is_account_including_child_subscriptions,
            is_leftside::boolean                    AS is_leftside_account,
            is_summary::boolean                     AS is_summary_account

    FROM source

)

SELECT *
FROM renamed
