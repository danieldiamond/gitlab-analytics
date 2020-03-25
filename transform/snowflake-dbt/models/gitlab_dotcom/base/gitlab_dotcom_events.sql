{{ config({
    "materialized": "incremental",
    "unique_key": "event_id",
    "schema": "analytics"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'events') }}

      {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

      {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}

), plans AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

), users AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }}
  
), renamed AS (

    SELECT
      id                                                              AS event_id,
      project_id::INTEGER                                             AS project_id,
      author_id::INTEGER                                              AS author_id,
      target_id::INTEGER                                              AS target_id,
      target_type::VARCHAR                                            AS target_type,
      created_at::TIMESTAMP                                           AS created_at,
      updated_at::TIMESTAMP                                           AS updated_at,
      action::INTEGER                                                 AS event_action_type_id,
      {{action_type(action_type_id='event_action_type_id')}}::VARCHAR AS event_action_type

    FROM source

), joined AS (

    SELECT
      renamed.*,
      projects.ultimate_parent_id,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END                                 AS plan_id_at_event_date,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(plans.plan_name, 'free')
      END                                 AS plan_name_at_event_date,
      COALESCE(plans.plan_is_paid, FALSE) AS plan_was_paid_at_event_date,
      users.created_at                    AS user_created_at
    FROM renamed
      LEFT JOIN projects
        ON renamed.project_id = projects.project_id
      LEFT JOIN gitlab_subscriptions
        ON projects.ultimate_parent_id = gitlab_subscriptions.namespace_id
        AND renamed.created_at BETWEEN gitlab_subscriptions.valid_from
        AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
      LEFT JOIN plans
        ON gitlab_subscriptions.plan_id = plans.plan_id
      LEFT JOIN users
        ON renamed.author_id = users.user_id

)

SELECT *
FROM joined
ORDER BY updated_at
