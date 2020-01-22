{% if execute %}

    {% set columns = graph.nodes['model.gitlab_snowflake.sfdc_lead']['columns'].keys()  %}
    {%  do log("Description: " ~ columns, info=true) %}

{% endif %}

WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_lead') }}

), sfdc_lead_pii AS (

    SELECT
        {% for column in columns %}

        sha1({{column}}) AS {{ column }}_hash

            {%- if not loop.last %}
                ,
            {% endif -%}

        {% endfor %}
    FROM source

)

SELECT *
FROM sfdc_lead_pii
