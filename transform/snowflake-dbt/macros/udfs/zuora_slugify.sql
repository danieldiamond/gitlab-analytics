{% macro zuora_slugify() %}
CREATE OR REPLACE FUNCTION {{target.schema}}.zuora_slugify("input_text" text)
  RETURNS TEXT
AS $$
  WITH   replace_chars AS (
    SELECT regexp_replace(input_text, '[^\w\s-]', '-', 'i') AS value
  ),
  lowercase AS (
    SELECT lower(value) AS value
    FROM replace_chars
  ),
  trimmed AS (
    SELECT trim(value) AS value
    FROM lowerase
  ),
  hyphenated AS (
    SELECT regexp_replace(value, '[\s]+', '-', 'i') AS value
    FROM trimmed
  )
  SELECT value FROM hyphenated;
$$;
{% endmacro %}