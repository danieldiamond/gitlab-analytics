with sfdc_ps_engagement AS (

    SELECT * 
    FROM {{ref('sfdc_professional_services_engagement')}}

), sfdc_users AS (

    SELECT * 
    FROM {{ref('sfdc_users_xf')}}

), joined as (

    SELECT 
      sfdc_ps_engagement.*,
      sfdc_users.name         AS ps_engagement_owner,
      sfdc_users.manager_name AS ps_engagement_owner_manager,
      sfdc_users.department   AS ps_engagement_owner_department,
      sfdc_users.title        AS ps_engagement_owner_title
    FROM sfdc_ps_engagement
    LEFT JOIN sfdc_users
      ON sfdc_ps_engagement.owner_id = sfdc_users.id

)

SELECT * 
FROM joined