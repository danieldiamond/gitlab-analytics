WITH model AS (

    SELECT 
      billing_list_price
    FROM {{ ref('dim_product_details') }}

)
SELECT billing_list_price
FROM model
WHERE billing_list_price < 0