WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespace_root_storage_statistics') }}

), renamed AS (

    SELECT

      namespace_id::INTEGER         AS namespace_id,
      repository_size::INTEGER      AS repository_size,
      lfs_objects_size::INTEGER     AS lfs_objects_size,
      wiki_size::INTEGER            AS wiki_size,
      build_artifacts_size::INTEGER AS build_artifacts_size,
      storage_size::INTEGER         AS storage_size,
      packages_size::INTEGER        AS packages_size,
      updated_at::timestamp         AS namespace_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
