{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'opportunity_line_item') }}



), renamed AS(

    SELECT

        --id
        id                                                      AS opportunity_line_item_id,
        name                                                    AS opportunity_line_item_name,
        description                                             AS opportunity_line_item_description,

        --keys
        opportunityid                                           AS opportunity_id,
        pricebookentryid                                        AS price_book_entry_id,
        product2id                                              AS product_id,
        opportunity_product_id__c                               AS opportunity_product_id,

        --info
        product_code_from_products__c                           AS product_code_from_products,
        product_name_from_products__c                           AS product_name_from_products,
        listprice                                               AS list_price,
        productcode                                             AS product_code,
        quantity                                                AS quantity,
        servicedate                                             AS service_date,
        sortorder                                               AS sort_order,
        ticket_group_numeric__c                                 AS ticket_group_numeric,
        totalprice                                              AS total_price,
        unitprice                                               AS unit_price,

        --metadata
        createdbyid                                             AS created_by_id,
        createddate                                             AS created_date,
        lastmodifiedbyid                                        AS last_modified_id,
        lastmodifieddate                                        AS last_modified_date,
        systemmodstamp


    FROM source

)

SELECT *
FROM renamed