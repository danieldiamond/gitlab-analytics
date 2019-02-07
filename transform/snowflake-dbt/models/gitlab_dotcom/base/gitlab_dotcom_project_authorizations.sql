WITH source AS (

	SELECT DISTINCT user_id, project_id, access_level
	FROM raw.gitlab_dotcom.project_authorizations

), renamed AS (

    SELECT

      md5(user_id :: integer || '-' || project_id :: integer || '-' || access_level :: integer) as user_project_access_relation_id, -- without the extra '-' two rows result in the same hash
      user_id :: integer                                                                        as user_id,
      project_id :: integer                                                                     as project_id,
      access_level :: integer                                                                   as access_level

    FROM source

) SELECT * from renamed