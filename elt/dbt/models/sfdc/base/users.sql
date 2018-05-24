SELECT
  u.name,
  u.department,
  u.title,
  u.team__c AS team,
  u.email,
  u.id,
  u2.name   AS manager_name,
  u2.id     AS manager_id,
  r.name    AS role_name
FROM sfdc.user u
  JOIN sfdc.userrole r ON userroleid = r.id
  JOIN sfdc.user u2 ON u2.id = u.managerid
WHERE u.isactive = TRUE