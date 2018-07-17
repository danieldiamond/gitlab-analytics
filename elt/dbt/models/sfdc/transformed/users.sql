SELECT
  u.name,
  u.department,
  u.title,
  u.team__c AS team,
  u.email,
  u.id,
  u2.name   AS manager_name,
  u2.id     AS manager_id,
  r.name    AS role_name,
  u.isactive AS is_active,
  CASE
    -- Active Employees
     WHEN u.id='005610000024KxAAAU' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='005610000024TeRAAU' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='00561000002421SAAQ' THEN 'Channel'
     WHEN u.id='005610000024E1AAAU' THEN 'Channel'
     WHEN u.id='005610000024ftUAAQ' THEN 'Large/Strategic;Account Executive;Public Sector'
     WHEN u.id='005610000024ESfAAM' THEN 'Channel'
     WHEN u.id='00561000000RzbDAAS' THEN 'Other'
     WHEN u.id='00561000001kBdoAAE' THEN 'Other'
     WHEN u.id='00561000002r61jAAA' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='00561000002rCsKAAU' THEN 'Large/Strategic;Account Executive;Public Sector'
     WHEN u.id='00561000002TReeAAG' THEN 'Channel'
     WHEN u.id='005610000024bhsAAA' THEN 'EMEA;BDR'
     WHEN u.id='00561000002rtfdAAA' THEN 'Mid-Market;Account Manager;EMEA'
     WHEN u.id='00561000002r4KyAAI' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='0056100000248cAAAQ' THEN 'Channel'
     WHEN u.id='005610000022r3LAAQ' THEN 'SDR'
     WHEN u.id='00561000002TPp6AAG' THEN 'Channel'
     WHEN u.id='00561000002TMAyAAO' THEN 'Channel'
     WHEN u.id='00561000001jx7OAAQ' THEN 'Other'
     WHEN u.id='00561000002TSZaAAO' THEN 'Channel'
     WHEN u.id='00561000002TYx4AAG' THEN 'Channel'
     WHEN u.id='00561000000hjpWAAQ' THEN 'Regional Director;US West'
     WHEN u.id='0056100000247DJAAY' THEN 'Channel'
     WHEN u.id='005610000023d63AAA' THEN 'Mid-Market;Account Manager;US West'
     WHEN u.id='005610000024nbOAAQ' THEN 'BDR'
     WHEN u.id='00561000002TNWRAA4' THEN 'SDR'
     WHEN u.id='00561000002rQobAAE' THEN 'Large/Strategic;Account Executive;Public Sector'
     WHEN u.id='00561000002rDxGAAU' THEN 'Large/Strategic;Account Executive;Channel'
     WHEN u.id='005610000022kyPAAQ' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='00561000002rpPMAAY' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='005610000024OFWAA2' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='00561000002TnHSAA0' THEN 'Mid-Market;Account Executive;US East'
     WHEN u.id='00561000001jx7TAAQ' THEN 'Mid-Market;Account Manager;APAC'
     WHEN u.id='00561000002rjBnAAI' THEN 'Large/Strategic;Account Executive;EMEA'
     WHEN u.id='005610000022r4EAAQ' THEN 'Other;BDR'
     WHEN u.id='00561000002rtyaAAA' THEN 'Web Direct'
     WHEN u.id='00561000002rKYwAAM' THEN 'BDR'
     WHEN u.id='00561000002TCwOAAW' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='0056100000243zJAAQ' THEN 'Channel'
     WHEN u.id='005610000024EckAAE' THEN 'Channel'
     WHEN u.id='005610000024Nb2AAE' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='0056100000246tdAAA' THEN 'US East;Regional Director'
     WHEN u.id='00561000002TkoiAAC' THEN 'Mid-Market;Account Executive;US West'
     WHEN u.id='00561000000bpPDAAY' THEN 'Regional Director;APAC'
     WHEN u.id='00561000002rcAsAAI' THEN 'SDR'
     WHEN u.id='00561000001jamXAAQ' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='005610000023GzFAAU' THEN 'BDR'
     WHEN u.id='00561000002r0euAAA' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='00561000002494sAAA' THEN 'Channel'
     WHEN u.id='005610000024WEMAA2' THEN 'Public Sector;Regional Director'
     WHEN u.id='00561000002rcAxAAI' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='00561000002494iAAA' THEN 'Channel'
     WHEN u.id='005610000024IRqAAM' THEN 'Mid-Market;Account Manager;US East'
     WHEN u.id='00561000000hhi3AAA' THEN 'Mid-Market;Account Executive;US West'
     WHEN u.id='00561000002rjn3AAA' THEN 'Large/Strategic;Account Executive;Public Sector'
     WHEN u.id='00561000000hjplAAA' THEN 'Regional Director;EMEA'
     WHEN u.id='0056100000248wAAAQ' THEN 'Channel'
     WHEN u.id='00561000000mpHTAAY' THEN 'Web Direct'
     WHEN u.id='005610000024bhnAAA' THEN 'Account Manager;EMEA'
     WHEN u.id='00561000002TNJrAAO' THEN 'Channel'
     WHEN u.id='0056100000243pJAAQ' THEN 'Channel'
     WHEN u.id='00561000000bTg9AAE' THEN 'Mid-Market;Account Executive;EMEA'
     WHEN u.id='00561000002rUozAAE' THEN 'Large/Strategic;Account Executive;EMEA'
     WHEN u.id='00561000002rqfpAAA' THEN 'Large/Strategic;Account Executive;US East'
     WHEN u.id='00561000002rs9RAAQ' THEN 'Large/Strategic;Account Executive;Channel'
     WHEN u.id='0056100000248nDAAQ' THEN 'Channel'
     -- Inactive Employees
     WHEN u.id='005610000023GCNAA2' THEN 'SDR'
     WHEN u.id='00561000001kBdoAAE' THEN 'Other'
     WHEN u.id='00561000001k60HAAQ' THEN 'Mid-Market;Account Executive;EMEA'
     WHEN u.id='005610000023zixAAA' THEN 'Large/Strategic;Account Executive;US West'
     WHEN u.id='00561000000hjosAAA' THEN 'Mid-Market;Account Executive;US West'
     WHEN u.id='00561000002TRvzAAG' THEN 'SDR'
     WHEN u.id='005610000023KIoAAM' THEN 'SDR'
     WHEN u.id='00561000002rKYrAAM' THEN 'SDR'
     WHEN u.id='005610000024L83AAE' THEN 'SDR'
     WHEN u.id='005610000024L7yAAE' THEN 'Large/Strategic;Account Executive;US East'
     ELSE NULL END AS employee_tags
FROM sfdc.user u
  LEFT OUTER JOIN sfdc.userrole r ON userroleid = r.id
  LEFT OUTER JOIN sfdc.user u2 ON u2.id = u.managerid