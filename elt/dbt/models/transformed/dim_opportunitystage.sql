SELECT row_number() OVER (
                          ORDER BY Id) AS id,
       apiname,
       createdbyid,
       createddate,
       defaultprobability,
       description,
       forecastcategory,
       forecastcategoryname,
       id AS sfdc_id,
       isactive,
       isclosed,
       iswon,
       lastmodifiedbyid,
       lastmodifieddate,
       masterlabel,
       sortorder,
       systemmodstamp
FROM sfdc.opportunitystage