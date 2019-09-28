WITH base_posting_account_activity AS (

    SELECT *
    FROM {{ ref('netsuite_posting_account_activity') }}

), base_departments AS (

   SELECT *
   FROM {{ ref('netsuite_departments_xf') }}

), base_accounts AS (

   SELECT *
   FROM {{ref('netsuite_accounts_xf')}}

)

SELECT a.*,
       b.ultimate_department_id,
       b.ultimate_department_name,
       c.ultimate_account_number   
FROM base_posting_account_activity a
LEFT JOIN base_departments b
  ON a.department_id = b.department_id
LEFT JOIN base_accounts c
  ON a.account_id = c.account_id
