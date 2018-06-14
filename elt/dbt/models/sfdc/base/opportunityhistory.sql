SELECT
  oh.*,
  (date_part('day',oh.systemmodstamp - oh.createddate)* 24 +
    date_part('hour',oh.systemmodstamp - oh.createddate)) / 24.0 as days_in_stage,
  CASE WHEN
    oh.amount::DECIMAL < 5000
    THEN 'Small (<5k)'
  WHEN oh.amount::DECIMAL >= 5000 AND oh.amount::DECIMAL < 25000
    THEN 'Medium (5k - 25k)'
  WHEN oh.amount::DECIMAL >= 25000 AND oh.amount::DECIMAL < 100000
    THEN 'Big (25k - 100k)'
  WHEN oh.amount::DECIMAL >= 100000
    THEN 'Jumbo (>100k)'
  ELSE 'Unknown' END                                                                             AS deal_size
FROM sfdc.opportunityhistory oh