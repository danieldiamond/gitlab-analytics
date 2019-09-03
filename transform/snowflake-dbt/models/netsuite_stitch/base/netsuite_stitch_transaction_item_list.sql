{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT internalid                          AS transaction_id,
           account,
           department,
           entity,
           parse_json(itemlist['item'])        AS item_list
    FROM {{ source('netsuite_stitch', 'transaction') }}
    WHERE itemlist is not NULL

), flattened AS (

    SELECT transaction_id,
           i.value['amount']                           AS amount,
           i.value['item']['internalId']::NUMBER       AS item_id,
           i.value['item']['name']::STRING             AS item_name,
           account['internalId']::NUMBER               AS account_id,
           account['name']::STRING                     AS account_name,
           department['internalId']::NUMBER            AS department_id,
           department['name']::STRING                  AS department_name,
           entity['internalId']::NUMBER                AS entity_id,
           entity['name']::STRING                      AS entity_name,
           i.value['line']                             AS line_id
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => item_list) i

), renamed AS (

    SELECT {{ dbt_utils.surrogate_key('transaction_id', 'line_id') }} AS transaction_item_list_unique_id,
          *
    FROM flattened
    WHERE item_name != 'TEST 123'

)

SELECT *
FROM renamed
