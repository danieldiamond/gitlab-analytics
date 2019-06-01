{{
    config({
        "schema": "sensitive"
    })
}}

WITH source AS (

    SELECT
        internalid                          AS transaction_id,
        entity,
        parse_json(expenselist['expense'])  AS expense
    FROM {{ source('netsuite_stitch', 'transaction') }}
    WHERE expenselist is not NULL

), flattened AS (

    SELECT
        source.transaction_id,
        e.value:amount                              AS amount,
        e.value:account['internalId']::NUMBER       AS account_id,
        e.value:account['name']::STRING             AS account_name,
        e.value:category['internalId']::NUMBER      AS category_id,
        e.value:category['name']::STRING            AS category_name,
        e.value:customer['internalId']::NUMBER      AS customer_id,
        e.value:customer['name']::STRING            AS customer_name,
        e.value:department['internalId']::NUMBER    AS department_id,
        e.value:department['name']::STRING          AS department_name,
        entity['internalId']::NUMBER                AS entity_id,
        entity['name']::STRING                      AS entity_name,
        e.value:grossamt                            AS gross_amount,
        e.value:isbillable                          AS is_billable,
        e.value:line                                AS transaction_line_id,
        e.value:memo                                AS memo,
        e.value:tax1amt                             AS tax_1_amount,
        e.value:taxcode['internalId']::NUMBER       AS tax_code_id,
        e.value:taxcode['name']::STRING             AS tax_code_name,
        e.value:taxrate1                            AS tax_rate_1
    FROM source
    , LATERAL FLATTEN(INPUT => expense) e

), renamed AS (

    SELECT {{ dbt_utils.surrogate_key('transaction_id', 'transaction_line_id') }} AS transaction_expense_lines_unique_id,
      CASE
         WHEN lower(account_name) LIKE '%contract%'
            THEN substring(md5(entity_name), 16)
         ELSE entity_name END   AS entity_name,
      CASE
         WHEN lower(account_name) LIKE '%contract%'
            THEN substring(md5(memo), 16)
         ELSE memo END          AS memo,
      amount,
      account_id,
      account_name,
      category_id,
      category_name,
      customer_id,
      customer_name,
      department_id,
      department_name,
      entity_id,
      gross_amount,
      is_billable,
      transaction_id,
      transaction_line_id,
      tax_1_amount,
      tax_code_id,
      tax_code_name,
      tax_rate_1
    FROM flattened

)

SELECT *
FROM renamed
