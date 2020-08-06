WITH usage_data AS (

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

), renamed AS (

    SELECT
      id              AS usage_ping_id,
      date_id,
      uuid,
      host_id,
      license_md5,
      hostname,
      source_ip,
      source_ip_numeric,
      main_edition    AS edition,
      product_tier,
      cleaned_version AS version,
      is_pre_release,
      instance_user_count,
      license_plan,
      license_trial   AS is_trial,
      created_at,
      recorded_at,
      updated_at
    FROM calculated

)        

SELECT *
FROM renamed