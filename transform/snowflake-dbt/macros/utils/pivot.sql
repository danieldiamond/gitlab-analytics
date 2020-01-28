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


