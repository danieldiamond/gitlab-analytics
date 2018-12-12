{% set set_to_true = "IS NOT NULL THEN True END" %}

{% set bonus_types = ['discretionary_bonuses','marketing_prize_bonuses', 'merit_lump_sum_bonuses','quarterly_bonuses','referral_bonuses','signing_bonuses','summit_prize_bonuses'] %}

{% set bonus_types_mapping = [('discretionary_bonuses', "'Discretionary Bonus'"), ('marketing_prize_bonuses', "'Marketing Prize'"), ('merit_lump_sum_bonuses', "'Merit Lump Sum Award'"), ('quarterly_bonuses', "'Quarterly Bonus'"), ('referral_bonuses', "'Referral Bonus'"), ('signing_bonuses', "'Signing bonus'"), ('summit_prize_bonuses', "'Summit Prize Bonus'")] %}

{% set compensation_change_types = ['commission_increase_compensation_change', 'performance_compensation_change', 'merit_administration_compensation_change', 'merit_increase_compensation_change', 'promotion_compensation_change'] %}

{% set compensation_change_types_mapping = [('commission_increase_compensation_change',  "'Salary and Commission/Bonus Increase'"), ('performance_compensation_change', "'Performance'"), ('merit_administration_compensation_change', "'Merit Administration'"), ('merit_increase_compensation_change', "'Merit Increase'"), ('promotion_compensation_change', "'Promotion'")] %}
 
with hired_candidates as (

    SELECT distinct candidate_name, status, termination, candidate_average_score
    FROM {{ref('interviewing_data_hired_candidates')}}

    ), interviewing_data_performance_of_hire as (

    SELECT *
    FROM {{ref('interviewing_data_performance_of_hire')}}

{% for bonus_type in bonus_types_mapping -%}

    ), {{bonus_type[0]}}  as (

     SELECT distinct full_name, bonus_type
     FROM interviewing_data_performance_of_hire
     WHERE bonus_type = {{bonus_type[1]}}

{% endfor -%}

{% for compensation_change_type in compensation_change_types_mapping -%}

    ), {{compensation_change_type[0]}}  as (

     SELECT distinct full_name, bonus_type
     FROM interviewing_data_performance_of_hire
     WHERE compensation_change_reason = {{compensation_change_type[1]}}

{% endfor -%}

    ), experience_factor as (

     SELECT full_name, experience_factor
     FROM interviewing_data_performance_of_hire
     WHERE experience_factor != ''
     GROUP BY 1, 2

    )

SELECT hired_candidates.candidate_name,
        md5(UPPER(hired_candidates.candidate_name)) as candidate_id,
        hired_candidates.status, 
        hired_candidates.termination,
        hired_candidates.candidate_average_score as candidate_score,
        {% for bonus_type in bonus_types -%}
        CASE WHEN {{bonus_type}}.full_name {{ set_to_true }} as received_{{bonus_type}},
        {% endfor -%}
        CASE WHEN ( {% for bonus_type in bonus_types -%}
                        {{bonus_type}}.full_name IS NOT NULL 
                        {%- if not loop.last %} OR {%- endif %}
                    {% endfor -%} ) THEN True END as received_any_bonus,
        {% for compensation_change_type in compensation_change_types -%}
        CASE WHEN {{compensation_change_type}}.full_name {{ set_to_true }} as received_{{compensation_change_type}},
        {% endfor -%}
        CASE WHEN ( {% for compensation_change_type in compensation_change_types -%}
                        {{compensation_change_type}}.full_name IS NOT NULL 
                        {%- if not loop.last %} OR {%- endif %}
                    {% endfor -%} ) THEN True END as received_positive_compensation_change,
        experience_factor
FROM hired_candidates
{% for bonus_type in bonus_types -%}
LEFT JOIN {{bonus_type}}
    ON UPPER({{bonus_type}}.full_name) = UPPER(hired_candidates.candidate_name)
{% endfor -%}
{% for compensation_change_type in compensation_change_types -%}
LEFT JOIN {{compensation_change_type}}
    ON UPPER({{compensation_change_type}}.full_name) = UPPER(hired_candidates.candidate_name)
{% endfor -%}
LEFT JOIN experience_factor
    ON UPPER(experience_factor.full_name) = UPPER(hired_candidates.candidate_name)