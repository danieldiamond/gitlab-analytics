{% set table_list = [ref('sheetload_pulse_survey_configure_be'),
                     ref('sheetload_pulse_survey_configure_fe'),
                     ref('sheetload_pulse_survey_monitor_be'),
                     ref('sheetload_pulse_survey_monitor_fe')] %}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=table_list,
        column_override=none,
        exclude=none,
        source_column_name=none
    ) }}
)
SELECT  {{ dbt_utils.surrogate_key('pk_id', 'gitlab_group', 'team') }}          AS response_id,
        {{ dbt_utils.surrogate_key('survey_date', 'gitlab_group', 'team') }}    AS survey_id,
        date_trunc('week', survey_date) as survey_week,
        enthusiasm_about_work,
        manager_support,
        recommend_GitLab,
        gitlab_group,
        team
FROM unioned
