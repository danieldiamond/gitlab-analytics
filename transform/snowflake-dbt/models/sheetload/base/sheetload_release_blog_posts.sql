with source as (

    SELECT * 
    FROM {{ source('sheetload', 'release_blog_posts') }}

), renamed as (

    SELECT 
      release::VARCHAR              AS release,
      base_items::INTEGER           AS base_items,
      bug_items::INTEGER            AS bug_items,
      performance_items::INTEGER    AS performance_items
    FROM source

)

SELECT *
FROM renamed