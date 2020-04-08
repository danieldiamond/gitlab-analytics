{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'abm_account_baselines') }}

), renamed AS (

    SELECT
      "Date Added"::DATE                                    AS added_date,
      "Account ID"::VARCHAR                                 AS account_id,
      "GTM Strategy"::VARCHAR                               AS gtm_strategy,
      NULLIF("ABM Tier", '')::VARCHAR                       AS abm_tier,
      NULLIF("Account Name", '')::VARCHAR                   AS account_name,
      NULLIF("Parent Account", '')::VARCHAR                 AS parent_account,
      NULLIF("Website", '')::VARCHAR                        AS website,
      NULLIF("Domains", '')::VARCHAR	                    AS domains,
      NULLIF("Type", '')::VARCHAR                           AS type,
      NULLIF("Count of Contacts", '')::FLOAT	            AS count_of_contacts,
      NULLIF("Health Score", '')::VARCHAR                   AS health_score,
      NULLIF("Count of Opportunities", '')::FLOAT           AS count_of_opportunities,
      NULLIF("Number of Open Opportunities", '')::FLOAT	    AS number_of_open_opportunities,
      NULLIF("Count of Won Opportunities", '')::FLOAT       AS count_of_won_opportunities,
      NULLIF("Total Closed Won Amount (All-Time)", '')::FLOAT
                                                            AS total_closed_won_amount,
      NULLIF("Sum: Open New/Add-on IACV Opportunities", '')::FLOAT
                                                            AS open_new_add_on_iacv,
      NULLIF("Sum of Open Renewal Opportunities", '')::FLOAT
                                                            AS sum_open_renewal_opportunities,
      NULLIF("Support Level", '')::VARCHAR                  AS support_level,
      NULLIF("MQL_SMB_Goal", '')::FLOAT                     AS mql_smb_goal,
      NULLIF("GitLab.com user", '')::FLOAT                  AS gitlab_com_user,
      NULLIF("GitLab EE Customer", '')::FLOAT               AS gitlab_ee_customer,
      NULLIF("EE Basic Customer", '')::FLOAT                AS ee_basic_customer,
      NULLIF("EE Standard Customer", '')::FLOAT             AS ee_standard_customer,
      NULLIF("EE Plus Customer", '')::FLOAT                 AS ee_plus_customer,
      NULLIF("Concurrent EE Subscriptions", '')::FLOAT      AS concurrent_ee_subscriptions,
      NULLIF("Count of Active Subscriptions", '')::FLOAT    AS count_active_subscriptions,
      NULLIF("Using CE", '')::FLOAT                         AS using_ce,
      NULLIF("CE Instances", '')::FLOAT                     AS ce_instances,
      NULLIF("Active CE Users", '')::FLOAT                  AS active_ce_users,
      NULLIF("DemandBase: Score", '')::VARCHAR              AS demandbase_score,
      NULLIF("DemandBase: Account List", '')::VARCHAR       AS demandbase_account_list,
      NULLIF("DemandBase: Intent", '')::VARCHAR             AS demandbase_intent,
      NULLIF("DemandBase: Page Views", '')::FLOAT           AS demandbase_page_views,
      NULLIF("DemandBase: Sessions", '')::FLOAT             AS demandbase_sessions,
      NULLIF("DemandBase: Trending Onsite Engagement", '')::FLOAT
                                                            AS demandbase_trending_onsite_engagement,
      NULLIF("DemandBase: Trending Offsite Intent", '')::FLOAT
                                                            AS demandbase_trending_offsite_intent,
      NULLIF("Account Owner", '')::VARCHAR                  AS account_owner,
      NULLIF("Billing State/Province", '')::VARCHAR         AS billing_state_province

    FROM source

)

SELECT *
FROM renamed
