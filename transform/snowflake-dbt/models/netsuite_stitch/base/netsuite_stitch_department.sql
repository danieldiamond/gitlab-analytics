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
        parent['name']::STRING          AS parent_name,
        name
    FROM source

)

SELECT *
FROM renamed


