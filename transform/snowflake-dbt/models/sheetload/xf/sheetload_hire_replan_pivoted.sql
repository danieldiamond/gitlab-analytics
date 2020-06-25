

{{ dbt_utils.unpivot(ref('sheetload_hire_replan'), cast_to='varchar', exclude=['Departments']) }}
