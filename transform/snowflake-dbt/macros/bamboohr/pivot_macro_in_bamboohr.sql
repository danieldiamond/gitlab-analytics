{% macro pivot(column,
               values,
               alias=True,
               agg='sum',
               cmp='=',
               prefix='',
               suffix='',
               then_value=1,
               else_value=0) %}
  {% for v in values %}
    {{ agg }}(
      case
      when {{ column }} {{ cmp }} '{{ v }}'
        then {{ then_value }}
      else {{ else_value }}
      end
    )
    {% if alias %}
      as {{ adapter.quote(prefix ~ v ~ suffix) }}
    {% endif %}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}