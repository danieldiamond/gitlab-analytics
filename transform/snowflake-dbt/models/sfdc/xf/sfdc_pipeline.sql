with sfdc_opportunity as (

  SELECT *
  FROM {{ref('sfdc_opportunity_xf')}}

), final as (

    SELECT sales_type,
           stage_name,
           close_date,
           opportunity_owner,
           opportunity_name,
           SUM(forecasted_iacv) AS forecasted_iacv,
           COUNT(*) AS opps
    FROM sfdc_opportunity 
    WHERE close_date >= DATE_TRUNC('month',CURRENT_DATE)
    AND   sales_type != 'Reseller'
    AND   stage_name IN ('0-Pending Acceptance','1-Discovery','2-Scoping',
                        '3-Technical Evaluation','4-Proposal','5-Negotiating',
                        '6-Awaiting Signature')
    AND   is_deleted = FALSE
    GROUP BY 1, 2, 3, 4, 5

)

SELECT *
FROM final