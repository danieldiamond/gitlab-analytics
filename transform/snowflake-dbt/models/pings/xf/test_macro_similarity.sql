{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="analytics",
      identifier="pings_usage_data_unpacked" )%}

{% set dbt_relation=ref('pings_usage_data_unpacked') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="id"
) }} 
