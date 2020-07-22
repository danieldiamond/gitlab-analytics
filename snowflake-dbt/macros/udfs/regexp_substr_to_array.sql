{% macro regexp_substr_to_array() %}
CREATE OR REPLACE FUNCTION {{target.schema}}_staging.regexp_to_array("input_text" string,
                                                                     "regex_text" STRING)
  RETURNS array
  LANGUAGE JAVASCRIPT
  AS '

  var regex_constructor = new RegExp(regex_text, "g")
  matched_substr_array = input_text.match(regex_constructor);
  if (matched_substr_array == null){
	matched_substr_array = []
  }
  return matched_substr_array

';
{% endmacro %}
