WITH recruiting_xf AS (
  
SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."GREENHOUSE_RECRUITING_XF"

), job_post_id AS (
  
SELECT *
    FROM "ANALYTICS"."GREENHOUSE"."GREENHOUSE_JOB_POSTS_SOURCE"
    WHERE job_post_title = 'Global Self-Identification Survey'
  
), candidate_pool AS (

SELECT recruiting_xf.* 
    FROM recruiting_xf
    INNER JOIN job_post_id ON recruiting_xf.job_id = job_post_id.job_id 
  
), demographic_answers AS (
  
SELECT *
    FROM "RAW"."GREENHOUSE"."DEMOGRAPHIC_ANSWERS"

), demograhpic_answer_options AS (

SELECT *
    FROM "RAW"."GREENHOUSE"."DEMOGRAPHIC_ANSWER_OPTIONS"

), demographic_question_translations AS (
  
SELECT *
    FROM "RAW"."GREENHOUSE"."DEMOGRAPHIC_QUESTION_TRANSLATIONS"
  
), survey_answers AS (
  
SELECT
    application_id,
    created_at,
    updated_at,
    demographic_question_translations.name AS survey_question,
    demograhpic_answer_options.name AS survey_answer    
    FROM demographic_answers
    LEFT JOIN demograhpic_answer_options ON demographic_answers.demographic_question_id = demograhpic_answer_options.demographic_question_id
    LEFT JOIN demographic_question_translations ON demographic_answers.demographic_question_id = demographic_question_translations.demographic_question_id
  
), survey_results AS (

SELECT
    application_id,
    MIN(created_at) AS survey_sent,
    CASE WHEN updated_at IS NOT null
    AND survey_answer IS NOT null
    THEN 1
    ELSE 0
    END AS survey_completed
    FROM survey_answers
    GROUP BY 1,3

), min_stage AS (

SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."GREENHOUSE_STAGE_INTERMEDIATE"
    WHERE application_stage = 'Screen'
   
) 

SELECT
    DATE_TRUNC('DAY', application_date) AS application_date,
    DATE_TRUNC('DAY', min_stage.stage_entered_on) AS screen_date,
    candidate_pool.current_stage_name,
    DATE_TRUNC('DAY', survey_results.survey_sent) AS survey_sent,
    DATE_TRUNC('DAY', survey_results.survey_completed) AS survey_completed,
    candidate_pool.candidate_recruiter
    FROM candidate_pool
    LEFT JOIN survey_results ON candidate_pool.application_id = survey_results.application_id
    LEFT JOIN min_stage ON candidate_pool.application_id = min_stage.application_id
    WHERE application_date > '2020-04-30'
    AND hit_screening = '1'   
