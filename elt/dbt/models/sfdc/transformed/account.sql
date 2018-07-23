SELECT a.id AS sfdc_account_id,
       a.name,
       a.industry,
       a.TYPE,
       a.Sales_Segmentation__c,
       sfdc.id15to18(substring(a.ultimate_parent_account__c,11, 15)) AS ultimate_parent_account__c,
       p.Sales_Segmentation__c AS ultimate_parent_Sales_Segmentation,
       p.name AS ultimate_parent_name,
       CASE
           WHEN p.Sales_Segmentation__c IN('Large', 'Strategic')
                OR a.Sales_Segmentation__c IN('Large', 'Strategic') THEN TRUE
           ELSE FALSE
       END AS Is_LAU,
       a.health__c as health_score,
       a.health_score_reasons__c as health_score_reasons
FROM sfdc.account a
LEFT OUTER JOIN sfdc.account p ON sfdc.id15to18(substring(a.ultimate_parent_account__c,11, 15))=p.id
WHERE a.isdeleted=FALSE