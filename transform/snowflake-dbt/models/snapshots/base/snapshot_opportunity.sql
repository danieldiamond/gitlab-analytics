with raw_ss_opportunity as (

       -- This table has multiple entires for each day
       -- Join on the date but then only get the latest timestamp for that day
       SELECT opp1.*
       FROM dbt_archive.sfdc_opportunity_archived opp1
       LEFT JOIN dbt_archive.sfdc_opportunity_archived opp2
         ON opp1.opportunity_id = opp2.opportunity_id
         AND opp1._last_dbt_run::date = opp2._last_dbt_run::date
         AND opp1._last_dbt_run > opp2._last_dbt_run
       WHERE opp1.is_deleted=FALSE
         AND opp2._last_dbt_run IS NULL

), ss_opportunity as (

      SELECT opportunity_id AS sfdc_opportunity_id,
             record_type_id,
             _last_dbt_run::date AS snapshot_date,
             account_id,
             stage_name,
             lead_source,
             sales_type,
             close_date,
             generated_source,
             sales_segment,
             sales_qualified_date,
             sales_accepted_date,
             reason_for_loss,
             reason_for_loss_details,
             opportunity_name,
             owner_id,
             incremental_acv,
             acv,
             renewal_acv,
             total_contract_value,
             'stitch' AS origin --denote that these rows come from stitch

      FROM raw_ss_opportunity

), raw_legacy_ss_opportunity as (

      SELECT
      *,
      row_number()
      OVER (
        PARTITION BY
          id,
          snapshot_date
        ORDER BY snapshot_date DESC ) AS sub_row
    FROM "RAW"."GCLOUD_POSTGRES_STITCH"."SFDC_DERIVED_SS_OPPORTUNITY"
    WHERE isdeleted=False

), legacy_ss_opportunity as (

      SELECT id AS sfdc_opportunity_id,
             recordtypeid AS record_type_id,
             snapshot_date::date AS snapshot_date,
             accountid AS account_id,
             stagename AS stage_name,
             leadsource AS lead_source,
             TYPE as sales_type,
             closedate AS close_date,
             sql_source__c AS generated_source,
              COALESCE(
                  initcap(
                      COALESCE(sales_segmentation_employees_o__c, sales_segmentation_o__c)
                      ), 'Unknown')       AS sales_segment,
             sales_qualified_date__c AS sales_qualified_date,
             sales_accepted_date__c AS sales_accepted_date,
             reason_for_lost__c AS reason_for_loss,
             reason_for_lost_details__c AS reason_for_loss_details,
             name AS opportunity_name,
             ownerid AS owner_id,
             Incremental_ACV__c AS incremental_acv,
             ACV__c AS acv,
             Renewal_ACV__c AS renewal_acv,
             Amount AS total_contract_value,
             'legacy' AS origin --denote that these rows were loaded from cloudsql and the legacy sfdc extractor

      FROM raw_legacy_ss_opportunity
      WHERE sub_row = 1
      AND snapshot_date::date < '2019-01-01'::date

)

SELECT *
FROM ss_opportunity

UNION

SELECT *
FROM legacy_ss_opportunity
