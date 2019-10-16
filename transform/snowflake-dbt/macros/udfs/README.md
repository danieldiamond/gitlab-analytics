UDF stands for User-defined functions. They let you extend the system to perform operations that are not available through the built-in, system-defined functions provided by Snowflake. Snowflake currently supports two types of UDFs: SQL and JavaScript.

## regexp_substr_to_array("input_text", "regex_text" STRING)

This UDF returns the all substrings that match a regular expression in an array  ("regex_text") within a string ("input_text") If no match is found, returns an empty array.
