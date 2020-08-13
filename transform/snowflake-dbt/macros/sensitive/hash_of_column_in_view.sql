{% macro hash_of_column_in_view(column) %}

    {% set results = run_query("
        select ENCRYPT_RAW(
            to_binary('{{ get_salt(column|lower) }}', 'utf-8'), 
            to_binary('416C736F4E6F745365637265FFFFFFAB', 'HEX'), 
            to_binary('416C736F4E637265FFFFFFAB', 'HEX')
        )['ciphertext']::VARCHAR "
      ) 
    %}

    sha2(
        TRIM(
            LOWER(
                {{column|lower}} ||  '{{results.columns[0].values()[0]}}'
            )
        )
    ) AS {{column|lower}}_hash,

{% endmacro %}