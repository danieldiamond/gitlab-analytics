{{ _xf_witth_anonymized_fields(
                                base_model='gitlab_dotcom_environments',
                                fields_to_mask=['environment_name', 'slug'],
                                anonymization_key='project_id'
) }}

SELECT * 
FROM anonymised
