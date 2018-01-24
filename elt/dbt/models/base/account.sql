SELECT id AS sfdc_account_id,
       name,
       industry,
       TYPE 
FROM sfdc.account
WHERE isdeleted=FALSE