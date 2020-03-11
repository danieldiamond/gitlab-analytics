{{ config({
    "alias": "sfdc_account_snapshots",
    "schema": "staging"
    })
}}

{{ create_snapshot_base(
    source=source('snapshots', 'sfdc_account_snapshots'),
    primary_key='id',
    date_start='2019-10-01',
    date_part='day',
    snapshot_id_name='account_snapshot_id' 
    )
}}

SELECT *
FROM final