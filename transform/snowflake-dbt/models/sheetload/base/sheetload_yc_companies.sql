WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'yc_companies') }}

), renamed as (

    SELECT md5("Name"::varchar||"Batch"::varchar) AS company_id,
         "Name"::varchar                          AS company_name,
         "Batch"::varchar                         AS yc_batch,
         "Description"::varchar                   AS company_description
    FROM source
)

SELECT *
FROM renamed
