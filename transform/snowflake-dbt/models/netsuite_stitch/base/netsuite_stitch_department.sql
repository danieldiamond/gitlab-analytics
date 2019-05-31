WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'department') }}

), renamed AS (

    SELECT
        internalid              AS department_id,
        includechildren         AS include_children,
        isinactive              AS is_inactive,
        parse_json(
            subsidiarylist['recordRef'][0])
                ['internalId']  AS subsidiary_list,
        parent['internalId']    AS parent_id,
        parent['name']          AS parent_name,
        name
    FROM source

)

SELECT *
FROM renamed


