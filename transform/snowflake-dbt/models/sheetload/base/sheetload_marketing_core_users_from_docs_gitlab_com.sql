WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'marketing_core_users_from_docs_gitlab_com') }}

), renamed as (

    SELECT
        "Company_Name"::varchar                         AS company_name,
        "Total_Page_Count"::int                         AS total_page_count
    FROM source
)

SELECT *
FROM renamed
