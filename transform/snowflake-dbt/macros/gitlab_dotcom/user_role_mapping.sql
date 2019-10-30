{%- macro user_role_mapping(user_role) -%}

    CASE
      WHEN {{user_role}}::INTEGER = 1 THEN 'Software Developer'
      WHEN {{user_role}}::INTEGER = 2 THEN 'updated'
      WHEN {{user_role}}::INTEGER = 3 THEN 'closed'
      WHEN {{user_role}}::INTEGER = 4 THEN 'reopened'
      WHEN {{user_role}}::INTEGER = 5 THEN 'pushed'
      WHEN {{user_role}}::INTEGER = 6 THEN 'commented'
      WHEN {{user_role}}::INTEGER = 7 THEN 'merged'
      WHEN {{user_role}}::INTEGER = 8 THEN 'joined'
      WHEN {{user_role}}::INTEGER = 9 THEN 'left'
    END

{%- endmacro -%}

/*
<option value="software_developer">Software Developer</option>
<option value="development_team_lead">Development Team Lead</option>
<option value="devops_engineer">Devops Engineer</option>
<option value="systems_administrator">Systems Administrator</option>
<option value="security_analyst">Security Analyst</option>
<option value="data_analyst">Data Analyst</option>
<option value="product_manager">Product Manager</option>
<option value="product_designer">Product Designer</option>
<option value="other">Other</option></select>
*/