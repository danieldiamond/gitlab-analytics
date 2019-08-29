{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'department') }}

), renamed AS (

    SELECT
        internalid                      AS department_id,
        includechildren                 AS include_children,
        isinactive                      AS is_inactive,
        parse_json(
            subsidiarylist['recordRef'][0])
                ['internalId']::NUMBER  AS subsidiary_list,
        parent['internalId']::NUMBER    AS parent_id,
        coalesce(parent['name']::STRING, name) AS parent_name,
        name                            AS department_name
    FROM source

)

SELECT *
FROM renamed
