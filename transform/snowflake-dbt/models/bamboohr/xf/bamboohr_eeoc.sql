with mapping AS (

    {{ dbt_utils.unpivot(relation=ref('bamboohr_id_employee_number_mapping'), cast_to='varchar', 
       exclude=['employee_number', 'employee_id','first_name', 'last_name', 'hire_date', 'termination_date', 'greenhouse_candidate_id']) }}

), mapping_enhanced AS (

    SELECT 
      employee_id,
      field_name                         AS eeoc_field_name, 
      COALESCE(value, 'Not Identified')  AS eeoc_value
    FROM mapping

    UNION ALL

    SELECT 
      DISTINCT employee_id,
      'no_eeoc'                         AS eeoc_field_name,
      'no_eeoc'                         AS eeoc_value
    FROM mapping


) 

select * from mapping_enhanced