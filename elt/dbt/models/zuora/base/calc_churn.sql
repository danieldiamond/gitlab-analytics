{{
  config({
    "materialized":"table",
    "post-hook": [
       "SELECT 1 --SELECT zuora.calc_churn()",
       "SELECT 1 --SELECT zuora.calc_subacct_churn()"
    ]
  })
}}

SELECT rolname
FROM pg_roles
LIMIT 1