{% macro zuora_slugify() %}
CREATE OR REPLACE FUNCTION {{target.schema}}.zuora_slugify(input_text text)
  RETURNS TEXT
IMMUTABLE
LANGUAGE sql
AS $$
  WITH   "replace_chars" AS (
    SELECT regexp_replace("input_text", E'[^\\w\\s-]', '-', 'gi') AS "value"
  ),
  "lowercase" AS (
    SELECT lower("value") AS "value"
    FROM "replace_chars"
  ),
  "trimmed" AS (
    SELECT trim("value") AS "value"
    FROM "lowercase"
  ),
  "hyphenated" AS (
    SELECT regexp_replace("value", E'[\\s]+', '-', 'gi') AS "value"
    FROM "trimmed"
  )
  SELECT "value" FROM "hyphenated";
$$;
{% endmacro %}