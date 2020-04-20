WITH source as (

    SELECT *
      FROM {{ source('greenhouse', 'candidate_custom_fields') }}

), renamed as (

    SELECT
            --keys
            candidate_id::bigint                AS candidate_id,
            user_id::bigint                     AS greenhouse_user_id,

            --info
            custom_field::varchar               AS candidate_custom_field,
            float_value::double precision       AS candidate_custom_field_float_value,
            date_value::varchar::date           AS candidate_custom_field_date,
            display_value::varchar              AS candidate_custom_field_display_value,
            min_value::numeric                  AS candidate_custom_field_min_value,
            max_value::numeric                  AS candidate_custom_field_max_value,
            created_at::timestamp               AS candidate_custom_field_created_at,
            updated_at::timestamp               AS candidate_custom_field_updated_at

    FROM source

)

SELECT *
FROM renamed
