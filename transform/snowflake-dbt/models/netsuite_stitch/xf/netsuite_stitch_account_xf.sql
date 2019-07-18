{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}


with base_accounts as (
    SELECT *
    FROM {{ref('netsuite_stitch_account')}}
    WHERE account_code IS NOT NULL
)

SELECT a.*,
       b.account_code AS parent_account_code,
       CASE
         WHEN parent_account_code IS NOT NULL
           THEN parent_account_code::text || ' : ' || a.account_code::text
       ELSE
           a.account_code::text
       END AS unique_account_code,

       CASE
         WHEN parent_account_code IS NOT NULL
           THEN parent_account_code
       ELSE
           a.account_code
       END AS ultimate_account_code


FROM base_accounts AS a
  LEFT JOIN base_accounts AS b
    ON a.parent_id = b.account_id