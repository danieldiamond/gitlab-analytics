WITH sfdc_pov AS (

    SELECT * 
    FROM {{ ref('sfdc_proof_of_value') }}

), sfdc_users AS (

    SELECT * 
    FROM {{ref('sfdc_users_xf')}}

), joined AS (

    SELECT
      sfdc_pov.*,
      owner.name    AS pov_owner_name,
      solarch.name  AS solution_architect_name,
      tam.name      AS technical_account_manager_name
    FROM sfdc_pov
    LEFT JOIN sfdc_users AS owner 
      ON sfdc_pov.pov_owner_id = owner.id
    LEFT JOIN sfdc_users AS solarch 
      ON sfdc_pov.solutions_architect_id = solarch.id
    LEFT JOIN sfdc_users AS tam 
      ON sfdc_pov.technical_account_manager_id = tam.id

)

SELECT * 
FROM joined