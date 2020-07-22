WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'user_actions') }}

), renamed as (

	SELECT

                --keys
                id::bigint          AS user_action_id,
                job_id::bigint      AS job_id,
                user_id::bigint     AS user_id,

                --info
                type::varchar       AS user_action_type


	FROM source

)

SELECT *
FROM renamed
