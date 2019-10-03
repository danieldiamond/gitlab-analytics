WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'accounts') }}

), renamed AS (

    SELECT
      --Primary Key
      account_id::FLOAT                                   AS account_id,

      --Foreign Keys
      parent_id::FLOAT                                    AS parent_account_id,
      currency_id::FLOAT                                  AS currency_id,
      department_id::FLOAT                                AS department_id,

      --Info
      name::VARCHAR                                       AS account_name,
      full_name::VARCHAR                                  AS account_full_name,
      full_description::VARCHAR                           AS account_full_description,
      accountnumber::VARCHAR                              AS account_number,
      expense_type_id::FLOAT                              AS expense_type_id,
      type_name::VARCHAR                                  AS account_type,
      type_sequence::FLOAT                                AS account_type_sequence,
      openbalance::FLOAT                                  AS current_account_balance,
      cashflow_rate_type::VARCHAR                         AS cashflow_rate_type,
      general_rate_type::VARCHAR                          AS general_rate_type,

      --Meta
      isinactive::BOOLEAN                                 AS is_account_inactive,
      is_balancesheet::BOOLEAN                            AS is_balancesheet_account,
      is_included_in_elimination::BOOLEAN                 AS is_account_included_in_elimination,
      is_included_in_reval::BOOLEAN                       AS is_account_included_in_reval,
      is_including_child_subs::BOOLEAN                    AS is_account_including_child_subscriptions,
      is_leftside::BOOLEAN                                AS is_leftside_account,
      is_summary::BOOLEAN                                 AS is_summary_account

    FROM source

)

SELECT *
FROM renamed
