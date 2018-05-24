SELECT id AS sfdc_id,
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
       weighted_iacv__c
FROM sfdc.opportunity
WHERE isdeleted=FALSE