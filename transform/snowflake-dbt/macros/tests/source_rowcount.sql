{% macro source_rowcount(schema, table, count) %}

WITH source as (

    SELECT *
    FROM {{ source(schema, table) }}

), counts AS (

    SELECT count(*) as row_count
    FROM source

)

SELECT row_count
FROM counts
WHERE row_count < {{ count }}

{% endmacro %}
