WITH base_accounts AS (

    SELECT *
    FROM {{ref('netsuite_accounts')}}
    WHERE account_number IS NOT NULL

), ultimate_account AS (

    SELECT a.*,
           b.account_number AS parent_account_number,
           CASE WHEN parent_account_number IS NOT NULL
                THEN parent_account_number || ' : ' || a.account_number
                ELSE a.account_number
           END                                      AS unique_account_number,
           CASE WHEN parent_account_number IS NOT NULL
                THEN parent_account_number
                ELSE a.account_number
           END                                      AS ultimate_account_number
    FROM base_accounts a
    LEFT JOIN base_accounts b
      ON a.parent_account_id = b.account_id

)

SELECT *
FROM ultimate_account
