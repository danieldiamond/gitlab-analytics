WITH source AS (

    SELECT 
      *,
      RANK() OVER (PARTITION BY date_trunc('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'roles') }}
    ORDER BY uploaded_at DESC

), filtered AS (

    SELECT *
    FROM source
    WHERE rank = 1

), intermediate AS (

    SELECT 
      d.value                                                                             AS data_by_row,
      date_trunc('day', uploaded_at)::date                                                AS snapshot_date
    FROM filtered,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), intermediate_role_information AS (

    SELECT
      snapshot_date,
      data_by_row['title']::varchar                                                       AS title,
      data_by_row['levels']::varchar                                                      AS role_levels,
      data_by_row['open']::varchar                                                        AS is_open,
      data_by_row['salary']::varchar                                                      AS previous_salary_value,
      data_by_row['ic_ttc']                                                               AS ic_values,
      data_by_row['manager_ttc']                                                          AS manager_values,
      data_by_row['director_ttc']                                                         AS director_values,
      data_by_row['senior_director_ttc']                                                  AS senior_director_values
    FROM intermediate

), renamed AS (

    SELECT 
      snapshot_date,
      title,
      role_levels,
      TRY_TO_BOOLEAN(is_open)                                                             AS is_open,
      TRY_TO_NUMERIC(previous_salary_value)                                               AS previous_salary_value,
      TRY_TO_NUMERIC(ic_values:compensation::varchar)                                     AS individual_contributor_compensation,
      TRY_TO_NUMERIC(ic_values:percentage_variable::varchar,5,2)                          AS individual_contributor_percentage_variable,
      TRY_TO_BOOLEAN(ic_values:from_base::varchar)                                        AS individual_contributor_from_base,
      TRY_TO_NUMERIC(manager_values:compensation::varchar)                                AS manager_compensation,
      TRY_TO_NUMERIC(manager_values:percentage_variable::varchar)                         AS manager_percentage_variable,
      TRY_TO_BOOLEAN(manager_values:from_base::varchar)                                   AS manager_from_base, 
      TRY_TO_NUMERIC(director_values:compensation::varchar)                               AS director_compensation,
      TRY_TO_NUMERIC(director_values:percentage_variable::varchar,5,2)                    AS director_percentage_variable,
      TRY_TO_BOOLEAN(director_values:from_base::varchar)                                  AS director_from_base, 
      TRY_TO_NUMERIC(senior_director_values:compensation::varchar)                        AS senior_director_compensation,
      TRY_TO_NUMERIC(senior_director_values:percentage_variable::varchar,5,2)             AS senior_director_percentage_variable,
      TRY_TO_BOOLEAN(senior_director_values:from_base::varchar)                           AS senior_director_from_base
    FROM intermediate_role_information
) 

SELECT *
FROM renamed
