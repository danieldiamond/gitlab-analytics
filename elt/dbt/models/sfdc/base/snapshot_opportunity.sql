SELECT id AS sfdc_opportunity_id,
       snapshot_date,
	accountid,
       stagename,
       leadsource,
       TYPE,
       closedate,
       sql_source__c,
       sales_segmentation_o__c,
       sales_qualified_date__c,
       sales_accepted_date__c,
       name,
       ownerid,
       Incremental_ACV__c AS iacv,
       ACV__c as ACV,
       Renewal_ACV__c as Renewal_ACV,
       Amount as TCV
FROM sfdc.ss_opportunity
WHERE isdeleted=FALSE