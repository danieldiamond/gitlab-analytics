{%- macro source_distinct_rows(source) -%}

source_distinct AS (

  SELECT
    {{ dbt_utils.star(from=source, except=['_UPLOADED_AT', '_TASK_INSTANCE']) }},
    MIN(DATEADD('sec', _uploaded_at, '1970-01-01')) AS valid_from,
    MAX(_task_instance) AS max_task_instance
  FROM source
  GROUP BY {{ dbt_utils.star(from=source, except=['_UPLOADED_AT', '_TASK_INSTANCE']) }}

)

{%- endmacro -%}
