WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespace_root_storage_statistics') }}

), renamed AS (

    SELECT

      namespace_id::integer         AS namespace_id,
      repository_size::integer      AS repository_size,
      lfs_objects_size::integer     AS lfs_objects_size,
      wiki_size::integer            AS wiki_size,
      build_artifacts_size::integer AS build_artifacts_size,
      storage_size::integer         AS storage_size,
      packages_size::integer        AS packages_size,
      updated_at::timestamp         AS namespace_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
