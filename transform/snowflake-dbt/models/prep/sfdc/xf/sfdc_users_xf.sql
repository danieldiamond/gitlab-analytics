with users as (

    SELECT * FROM {{ref('sfdc_users')}}

), user_role as (

    SELECT * FROM {{ref('sfdc_user_roles')}}

)

SELECT
  users.name          AS name,
  users.department    AS department,
  users.title         AS title,
  users.team__c       AS team,
  users.email         AS email,
  users.id            AS id,
  manager.name        AS manager_name,
  manager.id          AS manager_id,
  user_role.name      AS role_name,
  users.isactive      AS is_active
FROM users
LEFT OUTER JOIN user_role 
ON users.userroleid = user_role.id
LEFT OUTER JOIN users AS manager 
ON manager.id = users.managerid
