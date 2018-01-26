SELECT id AS sfdc_account_id,
       name,
       industry,
       TYPE,
       Sales_Segmentation__c
FROM sfdc.account
WHERE isdeleted=FALSE