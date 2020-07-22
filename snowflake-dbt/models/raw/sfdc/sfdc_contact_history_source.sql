WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'contact_history') }}

), renamed AS (

    SELECT
      contactid             AS contact_id,
      id                    AS contact_history_id,
      createddate           AS field_modified_at,
      LOWER(field)          AS contact_field,
      newvalue__fl          AS new_value_float,
      newvalue__de          AS new_value_decimal,
      oldvalue__fl          AS old_value_float,
      oldvalue__de          AS old_value_decimal,
      newvalue              as new_value,
      oldvalue              AS old_value,
      isdeleted             AS is_deleted,
      createdbyid           AS created_by_id
    FROM base

)

SELECT *
FROM renamed
