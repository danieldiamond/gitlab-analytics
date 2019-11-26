{%- macro scd_type_2(primary_key_renamed, primary_key_raw, source_cte='distinct_source', casted_cte='renamed') -%}

/* From the source table, for each primary_key find:
    * The maximum timestamp it was seen
    * Whether or not the row is in the most recent task */
, max_by_primary_key AS (
  SELECT
    {{ primary_key_raw }} AS primary_key,
    MAX(IFF(max_task_instance IN ( SELECT MAX(max_task_instance) FROM {{ source_cte }} ), 1, 0)) AS is_in_most_recent_task,
    MAX(valid_from)                                                                              AS max_timestamp
  FROM {{ source_cte }}
  GROUP BY 1

/* Select everything from the casted cte and add valid_to. The logic for valid_to is as follows:
   * If the primary_key has another row LATER than the current row, then the current row is valid up until one second before that.
   * If the row IS the most recent one for that 
*/
), windowed AS (
  SELECT
    {{casted_cte}}.*,

    COALESCE(
      DATEADD('millisecond', -1, FIRST_VALUE(valid_from) OVER (
        PARTITION BY {{casted_cte}}.{{primary_key_renamed}}
        ORDER BY valid_from
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
      ),
      IFF(is_in_most_recent_task = FALSE, max_by_primary_key.max_timestamp, NULL)
    ) AS valid_to,
    (valid_to IS NULL) AS is_currently_valid

  FROM {{casted_cte}}
    LEFT JOIN max_by_primary_key
      ON renamed.{{primary_key_renamed}} = max_by_primary_key.primary_key
  ORDER BY valid_from, valid_to

)

SELECT *
FROM windowed

{%- endmacro -%}
