WITH versions AS (

    SELECT *
    FROM {{ ref('version_versions_source') }}

), calculated AS (

    SELECT
      *,
      SPLIT_PART(version, '.', 1)::INT   AS major_version,
      SPLIT_PART(version, '.', 2)::INT   AS minor_version,
      SPLIT_PART(version, '.', 3)::INT   AS patch_number,
      IFF(patch_number = 0, TRUE, FALSE) AS is_monthly_release,
      created_at::DATE                   AS created_date,
      updated_at::DATE                   AS updated_date
    FROM versions  

), renamed AS (

    SELECT
      id AS version_id,
      version,
      major_version,
      minor_version,
      patch_number,
      is_monthly_release,
      is_vulnerable,
      created_date,
      updated_date
    FROM calculated  

)

SELECT *
FROM renamed

