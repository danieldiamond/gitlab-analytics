{{config({
    "schema": "staging"
  })
}}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'rate_plan') }}

), renamed AS(

  SELECT
    id                  AS rate_plan_id,
    name                AS rate_plan_name,
    --keys
    subscriptionid      AS subscription_id,
    productid           AS product_id,
    productrateplanid   AS product_rate_plan_id,
    -- info
    amendmentid         AS amendement_id,
    amendmenttype       AS amendement_type,

    --metadata
    updatedbyid         AS updated_by_id,
    updateddate         AS updated_date,
    createdbyid         AS created_by_id,
    createddate         AS created_date

  FROM source
  WHERE deleted = FALSE

), with_product_category AS (

  SELECT *,
    {{product_category('rate_plan_name')}},
    {{ delivery('product_category')}}
  FROM renamed
)

SELECT *
FROM with_product_category
