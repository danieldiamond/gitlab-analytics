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


{% docs map_state_id %}
This macro maps state_ids to english state names (opened, closed, etc).
{% enddocs %}


{% docs resource_event_action_type %}
This macro maps action type ID to the action type for the `resource_label_events` table.
{% enddocs %}


{% docs transform_clusters_applications %}
This macro takes in a ref (base model) and performs some joins to make an xf model out of the base model. This is used for all the `clusters_applications_*` tables as they all have the same structure and columns.
{% enddocs %}


{% docs user_role_mapping %}
This macro maps "role" values (integers) from the user table into their respective string values.

For example, user_role=0 maps to the 'Software Developer' role.

{% enddocs %}
