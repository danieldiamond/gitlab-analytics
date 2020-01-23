{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}

WITH usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data') }}

), licenses AS ( -- Licenses app doesn't alter rows after creation so the snapshot is not necessary.

    SELECT *
    FROM {{ ref('license_db_licenses') }}

), zuora_subscriptions AS (

    SELECT *
    FROM {{ ref('zuora_subscription')}}

), zuora_accounts AS (

    SELECT *
    FROM {{ ref('zuora_account')}}

), joined AS (

    SELECT
      usage_data.*,
      licenses.license_id,
      licenses.zuora_subscription_id,
      licenses.company,
      licenses.plan_code                      AS license_plan_code,
      licenses.license_starts_at,
      licenses.license_expires_at,
      zuora_subscriptions.subscription_status AS zuora_subscription_status,
      zuora_accounts.crm_id                   AS zuora_crm_id

    FROM usage_data
      LEFT JOIN licenses
        ON usage_data.license_md5 = licenses.license_md5
      LEFT JOIN zuora_subscriptions
        ON licenses.zuora_subscription_id = zuora_subscriptions.subscription_id
      LEFT JOIN zuora_accounts
        ON zuora_subscriptions.account_id = zuora_accounts.account_id
    WHERE licenses.email IS NULL
      OR NOT (email LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%') -- Exclude internal tests licenses.

), unpacked AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data'), except=['stats_used']) }},
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS'
        ELSE 'Self-Managed'
      END                                                                             AS ping_source,
      CONCAT(CONCAT(SPLIT_PART(version, '.', 1), '.'), SPLIT_PART(version, '.', 2))   AS major_version,
      CASE WHEN LOWER(edition) LIKE '%ee%' THEN 'EE'
        ELSE 'CE' END                                                                 AS main_edition,
      CASE WHEN edition LIKE '%CE%' THEN 'Core'
          WHEN edition LIKE '%EES%' THEN 'Starter'
          WHEN edition LIKE '%EEP%' THEN 'Premium'
          WHEN edition LIKE '%EEU%' THEN 'Ultimate'
          WHEN edition LIKE '%EE Free%' THEN 'Core'
          WHEN edition LIKE '%EE%' THEN 'Starter'
        ELSE NULL END                                                                 AS edition_type,
      zuora_subscription_id,
      zuora_subscription_status,
      zuora_crm_id,
      f.path                                                                          AS ping_name,
      REPLACE(f.path, '.','_')                                                        AS full_ping_name,
      f.value                                                                         AS ping_value

    FROM joined,
      lateral flatten(input => joined.stats_used, recursive => True) f
    WHERE IS_OBJECT(f.value) = FALSE
      AND stats_used IS NOT NULL
    {% if is_incremental() %}
        AND created_at > (SELECT max(created_at) FROM {{ this }})
    {% endif %}

), final AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data'), except=['stats_used']) }},
      unpacked.ping_source,
      unpacked.major_version,
      unpacked.main_edition,
      unpacked.edition_type,
      unpacked.zuora_subscription_id,
      unpacked.zuora_subscription_status,
      unpacked.zuora_crm_id,
    
      {% for stat_name in version_usage_stats_list %}
        MAX(IFF(full_ping_name = '{{stat_name}}', ping_value::NUMERIC, NULL)) AS {{stat_name}}
        {{ "," if not loop.last }}
      {% endfor %}
    FROM unpacked
    {{ dbt_utils.group_by(n=52) }}

)

SELECT *
FROM final
