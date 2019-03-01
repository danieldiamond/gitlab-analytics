{{ config(schema='analytics') }}

WITH sfdc_poc AS (

  SELECT * FROM {{ ref('sfdc_proof_of_concept') }}

), sfdc_users AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

)

  SELECT
      sfdc_poc.*,
      owner.name    as poc_owner_name,
      solarch.name  as solution_architect_name,
      tam.name      as technical_account_manager_name

  FROM sfdc_poc
  LEFT JOIN sfdc_users AS owner 
  ON sfdc_poc.poc_owner_id = owner.id
  LEFT JOIN sfdc_users AS solarch 
  ON sfdc_poc.solutions_architect_id = solarch.id
  LEFT JOIN sfdc_users AS tam 
  ON sfdc_poc.technical_account_manager_id = tam.id