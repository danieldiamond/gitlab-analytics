WITH reporting_data AS (

    SELECT *
    FROM {{ ref('paid_digital') }}


)


SELECT
    reporting_date                  AS reporting_date,
    medium                          AS medium,
    source                          AS source,
    campaign                        AS campaign,
    campaign_code                   AS campaign_code,
    UPPER(region)                   AS region,
    targeting                       AS targeting,
    ad_unit                         AS ad_unit,
    NULLIF(brand_not_brand, 'x')    AS brand_not_brand,
    NULLIF(match_unit, 'x')         AS match_unit,
    content                         AS content,
    team                            AS team,
    budget                          AS budget,
    data_source                     AS data_source,
    impressions                     AS impressions,
    clicks                          AS clicks,
    conversion                      AS conversion,
    cost                            AS cost,
    ga_conversion                   AS ga_conversion,
    compaign_code_type              AS compaign_code_type,
    content_type                    AS content_type,
    uploaded_at                     AS uploaded_at
FROM reporting_data
ORDER BY reporting_date
