with sfdc_statement_of_work AS (

    SELECT * FROM {{ref('sfdc_statement_of_work')}}

), sfdc_users AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), joined as (

    SELECT sfdc_statement_of_work.*,
            sfdc_users.name         AS sow_owner,
            sfdc_users.manager_name AS sow_owner_manager,
            sfdc_users.department   AS sow_owner_department,
            sfdc_users.title        AS sow_owner_title
    FROM sfdc_statement_of_work
    LEFT JOIN sfdc_users
    ON sfdc_users.id = sfdc_statement_of_work.owner_id

)

SELECT * 
FROM joined