WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'namespace_root_storage_statistics') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT

      namespace_id::INTEGER         AS namespace_id,
      repository_size::INTEGER      AS repository_size,
      lfs_objects_size::INTEGER     AS lfs_objects_size,
      wiki_size::INTEGER            AS wiki_size,
      build_artifacts_size::INTEGER AS build_artifacts_size,
      storage_size::INTEGER         AS storage_size,
      packages_size::INTEGER        AS packages_size,
      updated_at::TIMESTAMP         AS namespace_updated_at

    FROM source

)

SELECT *
FROM renamed
