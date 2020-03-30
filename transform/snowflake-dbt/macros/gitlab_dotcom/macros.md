{% docs action_type %}
This macro maps action type ID to the action type.
{% enddocs %}


{% docs get_internal_parent_namespaces %}
Returns a list of all the internal gitlab.com parent namespaces, enclosed in round brackets. This is useful for filtering an analysis down to external users only.

The internal namespaces are documented below.

| namespace | namespace ID |
| ------ | ------ |
| gitlab-com | 6543 |
| gitlab-org | 9970 |
| gitlab-data | 4347861 |
| charts | 1400979 |
| gl-recruiting | 2299361 |
| gl-frontend | 1353442 |
| gitlab-examples | 349181 |
| gl-secure | 3455548 |
| gl-retrospectives | 3068744 |
| gl-release | 5362395 |
| gl-docsteam-new | 4436569 |
| gl-legal-team | 3630110 |
| gl-locations | 3315282 |
| gl-serverless | 5811832 |
| gl-peoplepartners | 5496509 |
| gl-devops-tools | 4206656 |
| gl-compensation | 5495265 |
| gl-learning | 5496484 |
| meltano | 2524164 |

{% enddocs %}


{% docs resource_label_action_type %}
This macro maps action type ID to the action type for the `resource_label_events` table.
{% enddocs %}


{% docs user_role_mapping %}
This macro maps "role" values (integers) from the user table into their respective string values.

For example, user_role=0 maps to the 'Software Developer' role.

{% enddocs %}
