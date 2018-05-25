{{
  config({
    "materialized":"table",
    "post-hook": [
       "SELECT  zuora.calc_churn()"
    ]
  })
}}

SELECT rolname
FROM pg_roles
LIMIT 1