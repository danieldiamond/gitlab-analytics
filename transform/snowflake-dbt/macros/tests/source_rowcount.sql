{% macro source_rowcount(schema, table, count, where_clause=None) %}

WITH source as (

    SELECT *
    FROM {{ source(schema, table) }}

), counts AS (

    SELECT count(*) as row_count
    FROM source
    {% if where_clause != None %}
    WHERE {{ where_clause }}
    {% endif %}

)

SELECT row_count
FROM counts
WHERE row_count < {{ count }}

{% endmacro %}
