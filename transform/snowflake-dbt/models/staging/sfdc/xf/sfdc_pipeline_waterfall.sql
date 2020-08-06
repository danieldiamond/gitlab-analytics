WITH date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_opportunity_snapshot_history AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history') }}

), sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}

), beginning AS(

    SELECT
      d.fiscal_quarter_name_fy              AS close_qtr,
      d.fiscal_year                         AS fiscal_close_year,
      d.first_day_of_fiscal_quarter,
      COALESCE (o.order_type, '3. Growth')  AS order_type,
      CASE
        WHEN (a.ultimate_parent_account_segment = 'Unknown' OR a.ultimate_parent_account_segment IS NULL) AND o.user_segment = 'SMB'                                                           THEN 'SMB'
        WHEN (a.ultimate_parent_account_segment = 'Unknown' OR a.ultimate_parent_account_segment IS NULL) AND o.user_segment = 'Mid-Market'                                                    THEN 'Mid-Market'
        WHEN (a.ultimate_parent_account_segment = 'Unknown' OR a.ultimate_parent_account_segment IS NULL) AND o.user_segment IN ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC') THEN 'Large'
        ELSE a.ultimate_parent_account_segment
      END                                   AS sales_segment,
      h.stage_name,
      CASE
        WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')  THEN 'Pipeline'
        WHEN h.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                         THEN '3+ Pipeline'
        WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       THEN 'Lost'
        WHEN h.stage_name IN ('Closed Won')                                                                                                         THEN 'Closed Won'
        ELSE 'Other'
      END                                   AS stage_name_3plus,
      CASE
         WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     THEN 'Pipeline'
         WHEN h.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               THEN '4+ Pipeline'
         WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   THEN 'Lost'
         WHEN h.stage_name IN ('Closed Won')                                                                                                                                     THEN 'Closed Won'
         ELSE 'Other'
         END                                  AS stage_name_4plus,
         h.opportunity_id,
       CASE
         WHEN o.account_owner_team_stamped = 'US East'                                                                                       THEN 'US East'
         WHEN o.account_owner_team_stamped = 'US West'                                                                                       THEN 'US West'
         WHEN o.account_owner_team_stamped = 'EMEA'                                                                                          THEN 'EMEA'
         WHEN o.account_owner_team_stamped = 'APAC'                                                                                          THEN 'APAC'
         WHEN o.account_owner_team_stamped = 'Public Sector'                                                                                 THEN 'Public Sector'
         WHEN o.account_owner_team_stamped IN ('Commercial', 'Commercial - MM', 'MM - East', 'MM - West', 'MM-EMEA', 'MM - EMEA', 'MM-APAC') THEN 'MM'
         WHEN o.account_owner_team_stamped IN ('SMB', 'SMB - US', 'SMB - International', 'Commercial - SMB')                                 THEN 'SMB'
         ELSE 'Other'
       END                                  AS account_owner_team_stamped,
       DATE(h.created_date)                 AS created_date,
       DATE(h.close_date)                   AS close_date,
       COUNT(DISTINCT h.opportunity_id)     AS opps,
       SUM(CASE
             WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') AND h.sales_type = 'Renewal' THEN h.renewal_acv*-1
             WHEN h.stage_name IN ('Closed Won')                                                THEN h.forecasted_iacv
             ELSE 0
           END)                             AS net_iacv,
       SUM(h.forecasted_IACV)               AS forecasted_iacv,
       SUM(o.pre_covid_iacv)                AS pre_covid_iacv
     FROM sfdc_opportunity_snapshot_history h
     LEFT JOIN sfdc_opportunity_xf o
       ON h.opportunity_id = o.opportunity_id
     LEFT JOIN sfdc_accounts_xf a
       ON h.account_id = a.account_id
     INNER JOIN date_details d
       ON h.close_date = d.date_actual
     INNER JOIN date_details dd2
       ON h.date_actual = dd2.date_actual
     WHERE dd2.day_of_fiscal_quarter = 1
       AND d.quarter_number - dd2.quarter_number = 0
     {{ dbt_utils.group_by(n=12) }}

), ENDING AS (

    SELECT
      d.fiscal_quarter_name_fy                  AS close_qtr,
      d.fiscal_year                             AS fiscal_close_year,
      d.first_day_of_fiscal_quarter,
      COALESCE (o.order_type, '3. Growth')      AS order_type,
      CASE
        WHEN (a.ultimate_parent_account_segment = 'Unknown' OR a.ultimate_parent_account_segment IS NULL) AND o.user_segment = 'SMB'                                                            THEN 'SMB'
        WHEN (a.ultimate_parent_account_segment = 'Unknown' OR a.ultimate_parent_account_segment IS NULL) AND o.user_segment = 'Mid-Market'                                                     THEN 'Mid-Market'
        WHEN (a.ultimate_parent_account_segment = 'Unknown' OR a.ultimate_parent_account_segment IS NULL) AND o.user_segment IN ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC')  THEN 'Large'
        ELSE a.ultimate_parent_account_segment
      END                                       AS sales_segment,
      h.stage_name,
      CASE
        WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')  THEN 'Pipeline'
        WHEN h.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                         THEN '3+ Pipeline'
        WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       THEN 'Lost'
        WHEN h.stage_name IN ('Closed Won')                                                                                                         THEN 'Closed Won'
        ELSE 'Other'
      END                                       AS stage_name_3plus,
      CASE
        WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     THEN 'Pipeline'
        WHEN h.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               THEN '4+ Pipeline'
        WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   THEN 'Lost'
        WHEN h.stage_name IN ('Closed Won')                                                                                                                                     THEN 'Closed Won'
        ELSE 'Other'
      END                                       AS stage_name_4plus,
      h.opportunity_id,
      CASE
        WHEN o.account_owner_team_stamped = 'US East'                                                                                       THEN 'US East'
        WHEN o.account_owner_team_stamped = 'US West'                                                                                       THEN 'US West'
        WHEN o.account_owner_team_stamped = 'EMEA'                                                                                          THEN 'EMEA'
        WHEN o.account_owner_team_stamped = 'APAC'                                                                                          THEN 'APAC'
        WHEN o.account_owner_team_stamped = 'Public Sector'                                                                                 THEN 'Public Sector'
        WHEN o.account_owner_team_stamped IN ('Commercial', 'Commercial - MM', 'MM - East', 'MM - West', 'MM-EMEA', 'MM - EMEA', 'MM-APAC') THEN 'MM'
        WHEN o.account_owner_team_stamped IN ('SMB', 'SMB - US', 'SMB - International', 'Commercial - SMB')                                 THEN 'SMB'
        ELSE 'Other'
      END                                       AS account_owner_team_stamped,
      DATE(h.created_date)                      AS created_date,
      DATE(h.close_date)                        AS close_date,
      COUNT(DISTINCT h.opportunity_id)          AS opps,
      SUM(CASE
            WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') AND h.sales_type = 'Renewal'      THEN h.renewal_acv*-1
            WHEN h.stage_name IN ('Closed Won')                                                     THEN h.forecasted_iacv
            ELSE 0
          END)                                  AS net_iacv,
      SUM(h.forecasted_iacv)                    AS forecasted_iacv,
      SUM(o.pre_covid_iacv)                     AS pre_covid_iacv
    FROM sfdc_opportunity_snapshot_history h
    LEFT JOIN sfdc_opportunity_xf o
      ON h.opportunity_id = o.opportunity_id
    LEFT JOIN sfdc_accounts_xf a
      ON h.account_id = a.account_id
    INNER JOIN date_details d
      ON h.close_date = d.date_actual
    INNER JOIN date_details dd2
      ON h.date_actual = dd2.date_actual
    WHERE dd2.day_of_fiscal_quarter = 1
      AND d.quarter_number - dd2.quarter_number = -1
    {{ dbt_utils.group_by(n=12) }}

), combined AS (

    SELECT
      COALESCE (b.opportunity_id, e.opportunity_id)                             AS opportunity_id,
      COALESCE (b.close_qtr, e.close_qtr)                                       AS close_qtr,
      COALESCE (b.fiscal_close_year, e.fiscal_close_year)                       AS fiscal_close_year,
      COALESCE (b.first_day_of_fiscal_quarter, e.first_day_of_fiscal_quarter)   AS first_day_of_fiscal_quarter,
      b.order_type,
      e.order_type                                                              AS order_type_ending,
      b.sales_segment,
      e.sales_segment                                                           AS sales_segment_ending,
      b.account_owner_team_stamped,
      e.account_owner_team_stamped                                              AS account_owner_team_ending,
      b.stage_name,
      e.stage_name                                                              AS stage_name_ending,
      b.stage_name_3plus,
      e.stage_name_3plus                                                        AS stage_name_3plus_ending,
      b.stage_name_4plus,
      e.stage_name_4plus                                                        AS stage_name_4plus_ending,
      b.created_date,
      e.created_date                                                            AS created_date_ending,
      b.close_date,
      e.close_date                                                              AS close_date_ending,
      SUM(b.opps)                                                               AS opps,
      SUM(e.opps)                                                               AS opps_ending,
      SUM(b.pre_covid_iacv)                                                     AS c19,
      SUM(e.pre_covid_iacv)                                                     AS c19_ending,
      SUM(b.net_iacv)                                                           AS net_iacv,
      SUM(e.net_iacv)                                                           AS net_iacv_ending,
      SUM(b.forecasted_iacv)                                                    AS forecasted_iacv,
      SUM(e.forecasted_iacv)                                                    AS forecasted_iacv_ending
    FROM beginning b
    FULL OUTER JOIN ending e
      ON b.opportunity_id || b.close_qtr = e.opportunity_id || e.close_qtr
    {{ dbt_utils.group_by(n=20) }}

), waterfall AS (

    SELECT
      combined.*,
      CASE WHEN close_date IS NOT NULL THEN forecasted_iacv ELSE 0 END                                                                      AS starting_pipeline,
      CASE WHEN created_date_ending >= first_day_of_fiscal_quarter AND close_date_ending IS NOT NULL THEN forecasted_iacv_ending ELSE 0 END AS created_in_qtr,
      CASE WHEN created_date_ending < first_day_of_fiscal_quarter AND close_date IS NULL THEN forecasted_iacv_ending ELSE 0 END             AS pulled_in_from_other_qtr,
      CASE WHEN stage_name_ending = '8-Closed Lost' AND net_iacv_ending = 0 THEN -forecasted_iacv_ending ELSE 0 END                         AS closed_lost,
      CASE WHEN close_date_ending IS NULL THEN -forecasted_iacv ELSE 0 END                                                                  AS slipped_deals,
      ZEROIFNULL(-net_iacv_ending)                                                                                                          AS net_iacv_waterfall,
      CASE
        WHEN stage_name_ending = 'Closed Won' THEN 0
        WHEN stage_name_ending = '9-Unqualified' THEN 0
        WHEN stage_name_ending = '10-Duplicate' THEN 0
        WHEN stage_name_ending = '8-Closed Lost' THEN 0
        ELSE forecasted_iacv_ending
      END                                                                                                                                   AS ending_pipeline,
      CASE
        WHEN (stage_name_ending = '9-Unqualified' OR stage_name_ending = '10-Duplicate') AND close_date_ending IS NOT NULL
        THEN -forecasted_iacv_ending ELSE 0
      END                                                                                                                                   AS duplicate_unqualified


    FROM combined

), net_change_in_pipeline_iacv AS (

    SELECT
      waterfall.*,
      (ending_pipeline - (starting_pipeline + created_in_qtr + pulled_in_from_other_qtr + closed_lost + duplicate_unqualified + slipped_deals)) - net_iacv_waterfall
                                                                                                                                             AS  net_change_in_pipeline_iacv
    FROM waterfall

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['opportunity_id', 'close_qtr']) }} AS primary_key,
      opportunity_id,
      close_qtr,
      fiscal_close_year,
      first_day_of_fiscal_quarter,
      order_type,
      order_type_ending,
      sales_segment,
      sales_segment_ending,
      account_owner_team_stamped,
      account_owner_team_ending,
      stage_name,
      stage_name_ending,
      stage_name_3plus,
      stage_name_3plus_ending,
      stage_name_4plus,
      stage_name_4plus_ending,
      created_date,
      created_date_ending,
      close_date,
      close_date_ending,
      opps,
      opps_ending,
      c19,
      c19_ending,
      net_iacv,
      net_iacv_ending,
      forecasted_iacv,
      forecasted_iacv_ending,
      starting_pipeline,
      net_change_in_pipeline_iacv,
      created_in_qtr,
      pulled_in_from_other_qtr,
      net_iacv_waterfall,
      closed_lost,
      duplicate_unqualified,
      slipped_deals,
      ending_pipeline
    FROM net_change_in_pipeline_iacv

)

SELECT *
FROM final
WHERE close_date_ending >= '2019-11-01'
  OR close_date >= '2019-11-01'
