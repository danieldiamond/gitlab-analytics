WITH ip_ranges AS (
    SELECT
      parse_ip(ip_range_first_ip, 'inet')['ipv4'] AS ip_address_number_range_start,
      parse_ip(ip_range_last_ip, 'inet')['ipv4']  AS ip_address_number_range_end,
      ip_range_first_ip                           AS ip_range_first_ip,
      ip_range_last_ip                            AS ip_range_last_ip,
      geoname_id                                  AS location_id
    FROM {{ ref('maxmind_ip_ranges_source') }}
    WHERE geoname_id IS NOT NULL

)

SELECT *
FROM ip_ranges