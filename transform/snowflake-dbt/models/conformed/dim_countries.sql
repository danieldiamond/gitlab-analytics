WITH country_codes_csv AS (

    SELECT official_name_en AS country_name,
           "ISO3166-1-Alpha-2" AS country_code_2_char,
           "ISO3166-1-Alpha-3" AS country_code_3_char,
           "ISO3166-1-numeric" AS country_code_numeric
    FROM ref( {{ 'country-codes_csv.csv' }} )

)

SELECT *
FROM country_codes_csv;
