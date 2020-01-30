WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'product') }}

), renamed AS (

    SELECT
      --Primary Keys
<<<<<<< HEAD
      id::VARCHAR                       AS product_id,

      --Info
      name::VARCHAR                     AS product_name,
      sku::VARCHAR                      AS sku,
      description::VARCHAR              AS product_description,
      category::VARCHAR                 AS category,
      updatedbyid::VARCHAR              AS updated_by_id,
      updateddate::TIMESTAMP_TZ         AS updated_date
=======
      id::VARCHAR                   AS product_id,

      --Info
      name::VARCHAR                 AS product_name,
      sku::VARCHAR                  AS sku,
      description::VARCHAR          AS product_description,
      category::VARCHAR             AS category,
      updatedbyid::VARCHAR          AS updated_by_id,
      updateddate::TIMESTAMP_TZ     AS updated_date

>>>>>>> 7df100be211277b1c18b33e3fb5dae00989cc1a4
    FROM source

)

SELECT *
FROM renamed
