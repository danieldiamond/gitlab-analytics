WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'gitlab_slack_stats') }}

), renamed AS (

  SELECT
    date::DATE                                              AS entry_date,
    -- running totals
    full_members::INTEGER                                   AS full_members,
    guests::INTEGER                                         AS guests,
    public_channels_in_single_workspace::INTEGER            AS public_channels_in_single_workspace,
    total_membership::INTEGER                               AS total_membership,
    -- daily totals
    daily_active_members::INTEGER                           AS daily_active_members,
    daily_members_posting_messages::INTEGER                 AS daily_members_posting_messages,
    files_uploaded::INTEGER                                 AS files_uploaded,
    messages_in_dms::INTEGER                                AS messages_in_dms,
    messages_in_private_channels::INTEGER                   AS messages_in_private_channels,
    messages_in_public_channels::INTEGER                    AS messages_in_public_channels,
    messages_in_shared_channels::INTEGER                    AS messages_in_shared_channels,  
    messages_posted::INTEGER                                AS messages_posted,
    messages_posted_by_apps::INTEGER                        AS messages_posted_by_apps,
    messages_posted_by_members::INTEGER                     AS messages_posted_by_members,
    percent_of_messages_in_dms::FLOAT                       AS percent_of_messages_in_dms,
    percent_of_messages_in_private_channels::FLOAT          AS percent_of_messages_in_private_channels,
    percent_of_messages_in_public_channels::FLOAT           AS percent_of_messages_in_public_channels,
    percent_of_views_in_dms::FLOAT                          AS percent_of_views_in_dms,
    percent_of_views_in_private_channels::FLOAT             AS percent_of_views_in_private_channels,
    percent_of_views_in_public_channels::FLOAT              AS percent_of_views_in_public_channels,
    weekly_active_members::INTEGER                          AS weekly_active_members,
    weekly_members_posting_messages::INTEGER                AS weekly_members_posting_messages
  FROM source  

)

SELECT *
FROM renamed