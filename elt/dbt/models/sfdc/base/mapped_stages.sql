SELECT
  id,
  masterlabel,
  CASE
  WHEN id = '01J6100000Jf6oDEAR' -- 0-Pending Acceptance
    THEN '0-Pending Acceptance'
  WHEN id = '01J6100000G5Xj4EAF' -- BDR Qualified
    THEN '0-Pending Acceptance'
  WHEN id = '01J6100000Ip3TXEAZ' -- 0-Pre AE Qualified
    THEN '0-Pending Acceptance'
  WHEN id = '01J6100000B8YLFEA3' -- Discovery
    THEN '1-Discovery'
  WHEN id = '01J6100000Ip3ThEAJ' -- 1-Discovery
    THEN '1-Discovery'
  WHEN id = '01J6100000B8YLPEA3' -- Developing
    THEN '2-Scoping'
  WHEN id = '01J6100000Ip3TmEAJ' -- 2-Scoping
    THEN '2-Scoping'
  WHEN id = '01J6100000B8YLUEA3' -- Present Solution
    THEN '3-Technical Evaluation'
  WHEN id = '01J6100000Ip3TrEAJ' -- 3-Tehcnical Evaluation
    THEN '3-Technical Evaluation'
  WHEN id = '01J6100000Ip3UBEAZ' -- 4-Proposal
    THEN '4-Propoasl'
  WHEN id = '01J6100000B8YLZEA3' -- Negotiating
    THEN '5-Negotiating'
  WHEN id = '01J6100000IoytuEAB' -- 5-Negotiating
    THEN '5-Negotiating'
  WHEN id = '01J6100000B8YLjEAN' -- Verbal Commitment
    THEN '6-Awaiting Signature'
  WHEN id = '01J6100000IWdyOEAT' -- Awaiting Approval
    THEN '6-Awaiting Signature'
  WHEN id = '01J6100000JfUIpEAN' -- 6-Awaiting Signature
    THEN '6-Awaiting Signature'
  WHEN isclosed IS TRUE
    THEN '7-Closed'
  ELSE
    'Unmapped'
  END AS mapped_stage
FROM sfdc.opportunitystage