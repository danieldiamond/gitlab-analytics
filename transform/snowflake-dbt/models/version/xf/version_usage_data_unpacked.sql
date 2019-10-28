{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}

WITH usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data') }}

), licenses AS (

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
      licenses.zuora_subscription_id,
      zuora_subscriptions.subscription_status AS zuora_subscription_status,
      zuora_accounts.crm_id                   AS zuora_crm_id

    FROM usage_data
      LEFT JOIN licenses
        ON usage_data.license_md5 = licenses.license_md5
      LEFT JOIN zuora_subscriptions
        ON licenses.zuora_subscription_id = zuora_subscriptions.subscription_id
      LEFT JOIN zuora_accounts
        ON zuora_subscriptions.account_id = zuora_accounts.account_id

), unpacked AS (

    SELECT
      id,
      source_ip,
      version,
      installation_type,
      active_user_count,
      created_at,
      mattermost_enabled,
      uuid,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS'
        ELSE 'Self-Managed' END                                                       AS ping_source,
      edition,
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
      hostname,
      host_id,
      git_version,
      gitaly_servers,
      gitaly_version,
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
      id,
      source_ip,
      version,
      installation_type,
      active_user_count,
      created_at,
      mattermost_enabled,
      uuid,
      ping_source,
      edition,
      major_version,
      main_edition,
      edition_type,
      hostname,
      host_id,
      git_version,
      gitaly_servers,
      gitaly_version,
      {% for stat_name in version_usage_stats_list %}
        MAX(IFF(full_ping_name = '{{stat_name}}', ping_value::numeric, NULL)) AS {{stat_name}} {{ "," if not loop.last }}
      {% endfor %}

    FROM unpacked
    {{ dbt_utils.group_by(n=18) }}

)

SELECT *
FROM final
