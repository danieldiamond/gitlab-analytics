with source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'account') }}

), renamed AS (

    SELECT internalid                                   AS account_id,
           acctname                                     AS account_name,
           acctnumber                                   AS account_number,
           accttype [ 'value' ] :: STRING               AS account_type,

            -- keys
           currency [ 'internalId' ]  :: NUMBER         AS currency_id,
           department [ 'internalId' ] :: NUMBER        AS department_id,
           parent [ 'internalId' ] :: NUMBER            AS parent_id,

            -- info
           cashflowrate [ 'value' ] :: STRING           AS cash_flow_rate,
           category1099misc                             AS category_1099_misc,
           curdocnum                                    AS doc_number,
           currency ['name'] :: STRING                  AS currency_name,
           parent [ 'name' ] :: STRING                  AS parent_name,
           customfieldlist [ 'internalId' ] :: NUMBER   AS custom_field_list_id,
           customfieldlist [ 'name' ] :: STRING         AS custom_field_list_name,
           customfieldlist [ 'value' ] :: STRING        AS custom_field_list_value,
           custrecord155                                AS custom_record_155_expense_type,
            -- custrecord_fam_account_showinfixedasset
           department [ 'name' ] :: STRING              AS department_name,
           description,
           eliminate,
           generalrate [ 'value' ] :: STRING            AS general_rate,
           includechildren                              AS include_children,
           isinactive                                   AS is_inactive,
           revalue,
           subsidiarylist                               AS subsidiary_list,
           custrecord_bl001_bank_currency               AS customer_record_bank_currency
        FROM source
)

SELECT *
FROM renamed