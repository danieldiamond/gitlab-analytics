{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}


WITH journal_entries AS (
    SELECT *
    FROM {{ref('netsuite_stitch_journal_entries')}}
),

non_journal_entries AS (
    SELECT *
    FROM {{ref('netsuite_stitch_non_journal_entries')}}
)

SELECT *
FROM journal_entries

UNION ALL

SELECT *
FROM non_journal_entries
