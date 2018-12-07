with hired_candidates as (

     SELECT *
     FROM {{ref('interviewing_data_hired_candidates')}}

    ), discretionary_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Discretionary Bonus'

    ), experience_factor as (

     SELECT full_name, experience_factor
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE experience_factor != ''
     GROUP BY 1, 2

    )

SELECT *,
       CASE WHEN db.full_name IS NOT NULL THEN True END as received_discretionary_bonus,
       experience_factor.experience_factor
FROM hired_candidates
LEFT JOIN discretionary_bonuses
    ON UPPER(discretionary_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN experience_factor
    ON UPPER(experience_factor.full_name) = UPPER(hired_candidates.candidate_name)