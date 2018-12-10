with hired_candidates as (

     SELECT distinct candidate_name, status, termination, candidate_average_score
     FROM {{ref('interviewing_data_hired_candidates')}}

    ), discretionary_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Discretionary Bonus'

    ), marketing_prize_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Marketing Prize'

    ), merit_lump_sum_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Merit Lump Sum Award'

    ), quarterly_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Quarterly Bonus'

    ), referral_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Referral Bonus'

    ), signing_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Signing bonus'


    ), summit_prize_bonuses as (

     SELECT distinct full_name
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE bonus_type = 'Summit Prize Bonus'

    ), experience_factor as (

     SELECT full_name, experience_factor
     FROM {{ref('interviewing_data_performance_of_hire')}}
     WHERE experience_factor != ''
     GROUP BY 1, 2

    )

SELECT hired_candidates.candidate_name,
        md5(UPPER(hired_candidates.candidate_name)) as candidate_id,
        hired_candidates.status, 
        hired_candidates.termination,
        hired_candidates.candidate_average_score as candidate_score,
       CASE WHEN discretionary_bonuses.full_name IS NOT NULL THEN True END as received_discretionary_bonus,
       CASE WHEN marketing_prize_bonuses.full_name IS NOT NULL THEN True END as received_marketing_prize_bonus,
       CASE WHEN merit_lump_sum_bonuses.full_name IS NOT NULL THEN True END as received_merit_lump_sum_bonus,
       CASE WHEN quarterly_bonuses.full_name IS NOT NULL THEN True END as received_quarterly_bonus,
       CASE WHEN referral_bonuses.full_name IS NOT NULL THEN True END as received_referral_bonus,
       CASE WHEN signing_bonuses.full_name IS NOT NULL THEN True END as received_signing_bonus,
       CASE WHEN summit_prize_bonuses.full_name IS NOT NULL THEN True END as received_summit_prize_bonus,
       CASE WHEN (discretionary_bonuses.full_name IS NOT NULL OR marketing_prize_bonuses.full_name IS NOT NULL
        OR merit_lump_sum_bonuses.full_name IS NOT NULL OR quarterly_bonuses.full_name IS NOT NULL 
        OR referral_bonuses.full_name IS NOT NULL OR signing_bonuses.full_name IS NOT NULL 
        OR summit_prize_bonuses.full_name IS NOT NULL) THEN True END as received_any_bonus,
       experience_factor
FROM hired_candidates
LEFT JOIN discretionary_bonuses
    ON UPPER(discretionary_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN marketing_prize_bonuses
    ON UPPER(marketing_prize_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN merit_lump_sum_bonuses
    ON UPPER(merit_lump_sum_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN quarterly_bonuses
    ON UPPER(quarterly_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN referral_bonuses
    ON UPPER(referral_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN signing_bonuses
    ON UPPER(signing_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN summit_prize_bonuses
    ON UPPER(summit_prize_bonuses.full_name) = UPPER(hired_candidates.candidate_name)
LEFT JOIN experience_factor
    ON UPPER(experience_factor.full_name) = UPPER(hired_candidates.candidate_name)