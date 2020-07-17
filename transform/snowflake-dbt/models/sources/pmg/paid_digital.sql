WITH source AS (

    SELECT *
    FROM {{ source('pmg', 'paid_digital') }}

), intermediate AS (

    SELECT d.value as data_by_row, uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (
    SELECT
            data_by_row['date']::TIMESTAMP              AS reporting_date,
            data_by_row['utm_medium']::VARCHAR          AS medium,
            data_by_row['utm_source']::VARCHAR          AS source,
            data_by_row['utm_campaign']::VARCHAR        AS campaign,
            data_by_row['campaign_code']::VARCHAR       AS campaign_code,
            data_by_row['geo']::VARCHAR                 AS region,
            data_by_row['targeting']::VARCHAR           AS targeting,
            data_by_row['ad_unit']::VARCHAR             AS ad_unit,
            data_by_row['br_nb']::VARCHAR               AS brand_not_brand,
            data_by_row['match_unit']::VARCHAR          AS match_unit,
            data_by_row['content']::VARCHAR             AS content,
            data_by_row['team']::VARCHAR                AS team,
            data_by_row['budget']::VARCHAR              AS budget,
            data_by_row['data_source']::VARCHAR         AS data_source,
            data_by_row['impressions']::INTEGER         AS impressions,
            data_by_row['clicks']::INTEGER              AS clicks,
            data_by_row['conversions']::INTEGER         AS conversion,
            data_by_row['cost']::FLOAT                  AS cost,
            data_by_row['ga_conversions']::VARCHAR      AS ga_conversion,
            data_by_row['campaign_code_type']::VARCHAR  AS compaign_code_type,
            data_by_row['content_type']::VARCHAR        AS content_type,
            uploaded_at::TIMESTAMP                      AS uploaded_at
            FROM intermediate
)
SELECT * 
FROM parsed
