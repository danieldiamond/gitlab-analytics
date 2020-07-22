WITH RECURSIVE managers AS (

  SELECT
    id,
    name,
    role_name,
    manager_name,
    manager_id,
    0 AS level,
    '' AS path
  FROM {{ ref('sfdc_users_xf') }}
  WHERE role_name = 'CRO'
  
  UNION ALL
  
  SELECT
    users.id,
    users.name,
    users.role_name,
    users.manager_name,
    users.manager_id,
    level + 1,
    path || managers.role_name || '::'
  FROM {{ ref('sfdc_users_xf') }} users
  INNER JOIN managers
    ON users.manager_id = managers.id
  
), final AS (  

  SELECT
    id,
    name,
    role_name,
    manager_name,
    SPLIT_PART(path, '::', 1)::VARCHAR AS parent_role_1,
    SPLIT_PART(path, '::', 2)::VARCHAR AS parent_role_2,
    SPLIT_PART(path, '::', 3)::VARCHAR AS parent_role_3,
    SPLIT_PART(path, '::', 4)::VARCHAR AS parent_role_4,
    SPLIT_PART(path, '::', 5)::VARCHAR AS parent_role_5
  FROM managers
  
)

SELECT *
FROM final