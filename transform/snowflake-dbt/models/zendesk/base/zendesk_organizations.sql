{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'organizations') }}
),

renamed AS (

    SELECT

        --ids
        id                                                  AS organization_id,
        organization_fields['salesforce_id']::varchar       AS sfdc_account_id,

        --fields
        name                                                AS organization_name,
        tags                                                AS organization_tags,
        organization_fields['aar']::integer                 AS arr,
        organization_fields['market_segment']::varchar      AS organization_market_segment,

        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
