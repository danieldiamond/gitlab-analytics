SELECT id AS sfdc_id,
       stagename,
       TYPE,
       closedate
FROM sfdc.opportunity
WHERE isdeleted=FALSE