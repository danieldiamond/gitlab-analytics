{{ config({
    "alias": "sfdc_user_snapshots",
    "schema": "staging"
    })
}}

{{ create_snapshot_base(
    source=source('snapshots', 'sfdc_user_snapshots'),
    primary_key='id',
    date_start='2019-10-01',
    date_part='day',
    snapshot_id_name='user_snapshot_id'
    )
}}

SELECT *
FROM final