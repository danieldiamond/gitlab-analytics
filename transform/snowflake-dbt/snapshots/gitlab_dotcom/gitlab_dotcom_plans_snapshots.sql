{% snapshot gitlab_db_plans_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp_with_deletes',
          updated_at='updated_at',
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'plans') }}
    WHERE _task_instance IN (SELECT MAX(_task_instance) FROM {{ source('gitlab_dotcom', 'plans') }})
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

{% endsnapshot %}
