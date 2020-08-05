with source as (

    SELECT * 
    FROM {{ source('sheetload', 'release_blog_posts') }}

), renamed as (

    SELECT 
      release::VARCHAR             AS release,
      base_items::NUMBER           AS base_items,
      bug_items::NUMBER            AS bug_items,
      performance_items::NUMBER    AS performance_items
    FROM source

)

SELECT *
FROM renamed
