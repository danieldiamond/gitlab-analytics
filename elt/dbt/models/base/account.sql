SELECT a.id AS sfdc_account_id,
       a.name,
       a.industry,
       a.TYPE,
       a.Sales_Segmentation__c,
       sfdc.id15to18(substring(a.ultimate_parent_account__c,11, 15)) as ultimate_parent_account__c,
       p.Sales_Segmentation__c as ultimate_parent_Sales_Segmentation,
       p.name as ultimate_parent_name
FROM sfdc.account a
LEFT OUTER JOIN sfdc.account p ON sfdc.id15to18(substring(a.ultimate_parent_account__c,11, 15))=p.id
WHERE a.isdeleted=FALSE