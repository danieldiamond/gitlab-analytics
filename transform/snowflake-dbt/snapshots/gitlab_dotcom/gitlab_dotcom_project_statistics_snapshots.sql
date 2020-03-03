{% snapshot gitlab_dotcom_namespaces_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols=[
              'repository_size', 
              'commit_count', 
              'storage_size', 
              'repository_size',
              'lfs_objects_size', 
              'build_artifacts_size', 
              'shared_runners_seconds', 
              'shared_runners_seconds_last_reset',
          ],
        )
    }}
    
    WITH source as (

    	SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS statistics_rank_in_key
      
      FROM {{ source('gitlab_dotcom', 'project_statistics') }}

    )
    
    SELECT *
    FROM source
    WHERE statistics_rank_in_key = 1
    
{% endsnapshot %}