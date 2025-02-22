WITH source AS (

    SELECT *
    FROM {{ source('maxmind', 'ranges') }}

), parsed AS (

    SELECT
      network_start_ip::VARCHAR              AS ip_range_first_ip,
      network_last_ip::VARCHAR               AS ip_range_last_ip,
      geoname_id::NUMBER                     AS geoname_id,
      registered_country_geoname_id::NUMBER  AS registered_country_geoname_id,
      represented_country_geoname_id::NUMBER AS represented_country_geoname_id,
      is_anonymous_proxy::BOOLEAN            AS is_anonymous_proxy,
      is_satellite_provider::BOOLEAN         AS is_satellite_provider   
    FROM source

)

SELECT *
FROM parsed