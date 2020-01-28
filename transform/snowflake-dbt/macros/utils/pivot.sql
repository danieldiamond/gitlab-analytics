{% macro pivot(
               column,
               values,
               then_value,
               alias=True,
               agg='sum',
               cmp='=',
               prefix='',
               suffix='',
               else_value=0,
               quote_identifiers=False) %}

  {% for v in values %}
    {{ agg }}(
      case
      when {{ column }} {{ cmp }} '{{ v }}'
        then {{ then_value }}
      else {{ else_value }}
      end
    )
    {% if alias %}
      {% if quote_identifiers %}
            as {{ adapter.quote(prefix ~ v ~ suffix) }}
      {% else %}
        as {{prefix ~ v ~ suffix }}
      {% endif %}
    {% endif %}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}



{# Arguments:
    column: Column name, required
    values: List of row values to turn into columns, required
    then_value: Value to use if comparison succeeds
    alias: Whether to create column aliases, default is True
    agg: SQL aggregation function, default is sum
    cmp: SQL value comparison, default is =
    prefix: Column alias prefix, default is blank
    suffix: Column alias postfix, default is blank  
    else_value: Value to use if comparison fails, default is 0
    quote_identifiers: Whether to surround column aliases with double quotes, default is true 
    #}
