{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT
        internalid                          AS transaction_id,
        parse_json(linelist['line'])        AS line_list
    FROM {{ source('netsuite_stitch', 'transaction') }}
    WHERE linelist is not NULL

), flattened AS (

    SELECT
        source.transaction_id,
        l.value:amount                              AS amount,
        l.value:account['internalId']::NUMBER       AS account_id,
        l.value:account['name']::STRING             AS account_name,
        l.value:credit                              AS credit,
        l.value:debit                               AS debit,
        l.value:department['internalId']::NUMBER    AS department_id,
        l.value:department['name']::STRING          AS department_name,
        l.value:entity['internalId']::NUMBER        AS entity_id,
        l.value:entity['name']::STRING              AS entity_name,
        l.value:eliminate                           AS eliminate,
        l.value:line                                AS line_id,
        l.value:memo                                AS memo
    FROM source
    , LATERAL FLATTEN(INPUT => line_list) l

), renamed AS (

    SELECT {{ dbt_utils.surrogate_key('transaction_id', 'line_id') }} AS transaction_line_list_unique_id,
        *
    FROM flattened

)

SELECT *
FROM renamed
