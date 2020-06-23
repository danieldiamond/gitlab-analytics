{{ config({
    "schema": "analytics"
    })
}}

{{ dbt_utils.union_relations(
    relations=[ref('sheetload_ally_certificate'),
               ref('sheetload_values_certificate'),
               ref('sheetload_communication_certificate'),
               ref('ic_collaboration_competency'),
               ref('sheetload_compensation_certificate')]
) }}
