{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'subsidiary') }}

), renamed AS (

    SELECT
        checklayout['internalId']::NUMBER       AS check_layout_id,
        checklayout['name']::STRING             AS check_layout_name,
        country['value']::STRING                AS country,
        currency['internalId']::STRING          AS currency_name, -- Name is in ID place for some reason
        edition,
        email,
        federalidnumber                         AS federal_id_number,
        fiscalcalendar['internalId']::NUMBER    AS fiscal_calendar_id,
        fiscalcalendar['name']::STRING          AS fiscal_calendar_name,
        internalid                              AS subsidiary_id,
        iselimination                           AS is_elimination,
        isinactive                              AS is_inactive,
        legalname                               AS legal_name,
        mainaddress                             AS main_address,
        name,
        nexuslist                               AS nexus_list,
        pagelogo                                AS page_logo,
        parent['internalId']::NUMBER            AS parent_id,
        parent['name']::STRING                  AS parent_name,
        showsubsidiaryname                      AS subsidiary_name,
        ssnortin                                AS ssn_or_tin,
        state,
        state1taxnumber                         AS state_tax_number,
        taxfiscalcalendar['internalId']::NUMBER AS tax_fiscal_calendar_id,
        taxfiscalcalendar['name']::STRING       AS tax_fiscal_calendar_name,
        url
    FROM source

)

SELECT *
FROM renamed


