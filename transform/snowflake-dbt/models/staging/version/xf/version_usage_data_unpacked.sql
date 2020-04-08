{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}

WITH usage_data AS (

    SELECT {{ dbt_utils.star(from=ref('version_usage_data'), except=['LICENSE_ID', 'LICENSE_STARTS_AT', 'LICENSE_EXPIRES_AT']) }}
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

), version_releases AS (

    SELECT *
    FROM {{ ref('version_releases') }}

), joined AS (

    SELECT
      usage_data.*,
      licenses.license_id,
      licenses.zuora_subscription_id,
      licenses.company,
      licenses.plan_code                      AS license_plan_code,
      licenses.starts_at                      AS license_starts_at,
      licenses.license_expires_at,
      zuora_subscriptions.subscription_status AS zuora_subscription_status,
      zuora_accounts.crm_id                   AS zuora_crm_id,
      DATEDIFF('days', ping_version.release_date, usage_data.created_at)  AS days_after_version_release_date,
      latest_version.major_minor_version                                  AS latest_version_available_at_ping_creation,
      latest_version.version_row_number - ping_version.version_row_number AS versions_behind_latest

    FROM usage_data
      LEFT JOIN licenses
        ON usage_data.license_md5 = licenses.license_md5
      LEFT JOIN zuora_subscriptions
        ON licenses.zuora_subscription_id = zuora_subscriptions.subscription_id
      LEFT JOIN zuora_accounts
        ON zuora_subscriptions.account_id = zuora_accounts.account_id
      LEFT JOIN version_releases AS ping_version -- Join on the version of the ping itself.
        ON usage_data.major_minor_version = ping_version.major_minor_version
      LEFT JOIN version_releases AS latest_version -- Join the latest version released at the time of the ping.
        ON usage_data.created_at BETWEEN latest_version.release_date AND {{ coalesce_to_infinity('latest_version.next_version_release_date') }}
    WHERE
      (
        licenses.email IS NULL
        OR NOT (email LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%') -- Exclude internal tests licenses.
        OR uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      )

), unpacked AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data'), except=['stats_used']) }},
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS'
        ELSE 'Self-Managed'
      END                                                                             AS ping_source,
      CASE WHEN LOWER(edition) LIKE '%ee%' THEN 'EE'
        ELSE 'CE' END                                                                 AS main_edition,
      CASE WHEN edition LIKE '%CE%' THEN 'Core'
          WHEN edition LIKE '%EES%' THEN 'Starter'
          WHEN edition LIKE '%EEP%' THEN 'Premium'
          WHEN edition LIKE '%EEU%' THEN 'Ultimate'
          WHEN edition LIKE '%EE Free%' THEN 'Core'
          WHEN edition LIKE '%EE%' THEN 'Starter'
        ELSE NULL END                                                                 AS edition_type,
      license_plan_code,
      company,
      zuora_subscription_id,
      zuora_subscription_status,
      zuora_crm_id,
      days_after_version_release_date,
      latest_version_available_at_ping_creation,
      versions_behind_latest,
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
      unpacked.main_edition,
      unpacked.edition_type,
      unpacked.license_plan_code,
      unpacked.company,
      unpacked.zuora_subscription_id,
      unpacked.zuora_subscription_status,
      unpacked.zuora_crm_id,
      unpacked.days_after_version_release_date,
      unpacked.latest_version_available_at_ping_creation,
      unpacked.versions_behind_latest,

      {% for stat_name in version_usage_stats_list %}
        MAX(IFF(full_ping_name = '{{stat_name}}', ping_value::NUMERIC, NULL)) AS {{stat_name}}
        {{ "," if not loop.last }}
      {% endfor %}
    FROM unpacked
    {{ dbt_utils.group_by(n=60) }}

)

SELECT *
FROM final
