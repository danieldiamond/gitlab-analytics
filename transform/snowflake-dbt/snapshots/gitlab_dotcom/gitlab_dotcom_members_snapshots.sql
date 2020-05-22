{% snapshot gitlab_dotcom_members_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp_with_deletes',
          updated_at='created_at'
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'members') }}
    WHERE _task_instance IN (SELECT MAX(_task_instance) FROM {{ source('gitlab_dotcom', 'members') }})
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

{% endsnapshot %}
