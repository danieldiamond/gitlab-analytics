WITH journal_entries AS (
    SELECT *
    FROM {{ref('netsuite_journal_entries')}}
),

non_journal_entries AS (
    SELECT *
    FROM {{ref('netsuite_non_journal_entries')}}
)

SELECT *
FROM journal_entries

UNION ALL

SELECT *
FROM non_journal_entries