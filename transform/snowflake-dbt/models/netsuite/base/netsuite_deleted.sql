WITH base AS (
    SELECT *
    FROM raw.netsuite_stitch.netsuite_deleted

), renamed AS (

    SELECT
      internalid   AS internal_id,
      customrecord AS is_custom_record,
      deleteddate  AS deleted_timestamp,
      name         AS name,
      type         AS type
    FROM base

)

SELECT *
FROM renamed