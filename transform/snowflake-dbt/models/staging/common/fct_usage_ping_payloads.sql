WITH license AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

), product_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_source') }}

), rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}
    WHERE is_deleted = FALSE

), subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL

), calculated AS (

    SELECT
      *,
      TO_NUMBER(TO_CHAR(created_at::DATE,'YYYYMMDD'),'99999999') AS date_id,
      REGEXP_REPLACE(NULLIF(version, ''), '\-.*')                AS cleaned_version,
      IFF(
          version LIKE '%-pre%' OR version LIKE '%-rc%', 
          TRUE, FALSE
      )::BOOLEAN                                                 AS is_pre_release,
      IFF(edition = 'CE', 'CE', 'EE')                            AS main_edition,
      CASE
        WHEN edition IN ('CE', 'EE Free') THEN 'Core'
        WHEN edition IN ('EE', 'EES') THEN 'Starter'
        WHEN edition = 'EEP' THEN 'Premium'
        WHEN edition = 'EEU' THEN 'Ultimate'
      ELSE NULL END                                              AS product_tier,
      PARSE_IP(source_ip, 'inet')['ip_fields'][0]                AS source_ip_numeric
    FROM usage_data

), license_product_details AS (

    SELECT
      license.license_md5,
      subscription.subscription_id,
      subscription.account_id,
      ARRAY_AGG(DISTINCT product_rate_plan_charge_id)            AS array_product_details_id
    FROM license
    INNER JOIN subscription
      ON license.zuora_subscription_id = subscription.subscription_id
    INNER JOIN rate_plan
      ON subscription.subscription_id = rate_plan.subscription_id
    INNER JOIN product_rate_plan_charge
      ON rate_plan.product_rate_plan_id = product_rate_plan_charge.product_rate_plan_id
    GROUP BY 1,2,3      

), joined AS (

    SELECT
      calculated.*,
      subscription_id,
      account_id,
      array_product_details_id
    FROM calculated
    LEFT JOIN license_product_details
      ON calculated.license_md5 = license_product_details.license_md5  

), renamed AS (

    SELECT
      id              AS usage_ping_id,
      date_id,
      uuid,
      host_id,
      source_ip,
      source_ip_numeric,
      license_md5,
      subscription_id,
      account_id,
      array_product_details_id,
      hostname,
      main_edition    AS edition,
      product_tier,
      cleaned_version AS version,
      is_pre_release,
      instance_user_count,
      license_plan,
      license_trial   AS is_trial,
      created_at,
      recorded_at
    FROM joined

)  

SELECT *
FROM renamed