WITH all_journal_entries AS (

    SELECT *
    FROM {{ref('netsuite_stitch_journal_entries')}}

), all_non_journal_entries AS (

    SELECT *
    FROM {{ref('netsuite_stitch_non_journal_entries')}}

), unioned AS (

    SELECT *
    FROM all_journal_entries

    UNION ALL

    SELECT *
    FROM all_non_journal_entries

)

SELECT *
FROM unioned
WHERE account_name NOT IN ('5079 - Intercompany COGS','6079 - Intercompany')
AND (ultimate_account_code BETWEEN '5000' AND '6999' AND ultimate_account_code != '5079')