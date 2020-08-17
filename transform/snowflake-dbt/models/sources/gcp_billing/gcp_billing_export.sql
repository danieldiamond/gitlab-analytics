WITH source AS (

  SELECT *
  FROM {{ source('gcp_billing', 'gcp_billing_export_combined') }}

), renamed AS (

  SELECT
    flatten_export.value:billing_account_id::VARCHAR               AS billing_account_id,
    flatten_export.value:cost::NUMBER                              AS cost,
    flatten_export.value:cost_type::VARCHAR                        AS cost_type,
    flatten_export.value:credits::VARIANT                          AS credits,
    flatten_export.value:currency::VARCHAR                         AS currency,
    flatten_export.value:currency_conversion_rate::FLOAT           AS currency_conversion_rate,
    flatten_export.value:export_time::TIMESTAMP                    AS export_time,
    flatten_export.value:labels::VARIANT                           AS labels,
    flatten_export.value:location:country::VARCHAR                 AS resource_country,
    flatten_export.value:location:location::VARCHAR                AS resource_location,
    flatten_export.value:location:region::VARCHAR                  AS resource_region,
    flatten_export.value:location:zone::VARCHAR                    AS resource_zone,
    flatten_export.value:project:ancestry_numbers::VARCHAR         AS folder_id,
    flatten_export.value:project:id::VARCHAR                       AS project_id,
    flatten_export.value:project:labels::VARIANT                   AS project_labels,
    flatten_export.value:project:name::VARCHAR                     AS project_name,
    flatten_export.value:service:id::VARCHAR                       AS service_id,
    flatten_export.value:service:description::VARCHAR              AS service_description,
    flatten_export.value:sku:id::VARCHAR                           AS sku_id,
    flatten_export.value:sku:description::VARCHAR                  AS sku_description,
    flatten_export.value:system_labels::VARIANT                    AS system_labels,
    flatten_export.value:usage:amount::NUMBER                      AS usage_amount,
    flatten_export.value:usage:amount_in_pricing_units::NUMBER     AS usage_amount_in_pricing_units,
    flatten_export.value:usage:pricing_unit::VARCHAR               AS pricing_unit,
    flatten_export.value:usage:unit::VARCHAR                       AS usage_unit,
    flatten_export.value:usage_start_time::TIMESTAMP               AS usage_start_time,
    flatten_export.value:usage_end_time::TIMESTAMP                 AS usage_end_time
  FROM source,
  TABLE(FLATTEN(source.jsontext)) flatten_export

)


SELECT *
FROM renamed
