
---testing unpivot function--    
    
    {# {{ dbt_utils.unpivot(
        relation=ref('sheetload_hire_replan'),
        cast_to='integer',
        exclude =['department'])
    }} #}


 {# {{ dbt_utils.unpivot(relation=ref('bamboohr_id_employee_number_mapping'), cast_to='varchar', 
       exclude=['employee_number', 'employee_id','first_name', 'last_name', 'hire_date', 'termination_date', 'greenhouse_candidate_id']) }} #}

 {{ dbt_utils.unpivot(relation=ref('sheetload_hire_replan'), cast_to='varchar', 
       exclude=['department']) }}
