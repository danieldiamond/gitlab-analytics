{{ _xf_with_anonymized_fields(
                                base_model='gitlab_dotcom_epics',
                                fields_to_mask=['epic_title'],
                                anonymization_key='group_id'
) }}

SELECT * 
FROM anonymised
