-- depends_on: "ANALYTICS".analytics_staging.engineering_productivity_metrics_projects_to_include
-- depends_on: "ANALYTICS".analytics_staging.projects_part_of_product

WITH issues AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_issues')}}
    WHERE created_at >= DATEADD(year, -1, CURRENT_DATE())
  
), users AS (
  
    SELECT *
    {# user_id, users_name, user_name, is_external_user, organization, notification_email #}
    FROM {{ref('gitlab_dotcom_users')}}

), assignee AS (
  
    SELECT
      assignee.*, 
      user_name               AS assignee,
      notification_email      AS assignee_email,
      is_external_user        AS assignee_is_external_user
    FROM {{ref('gitlab_dotcom_issue_assignees')}} AS assignee
    LEFT JOIN users
      ON assignee.user_id = users.user_id  

), employees AS (
  
    SELECT *
    FROM {{ref('employee_directory_analysis')}}
    WHERE date_actual >= DATEADD(year, -1, CURRENT_DATE())
  
), label_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_label_links')}}
    WHERE is_currently_valid = True
      AND target_type = 'Issue'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      issues.issue_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM issues
    LEFT JOIN label_links
      ON issues.issue_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY issues.issue_id

), projects AS (

    SELECT
      project_id,
      namespace_id,
      visibility_level
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

 
), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')}}

), milestone AS (
  
    SELECT * 
    FROM {{ref('gitlab_dotcom_milestones')}}

), epic AS (
  
    SELECT 
      epic_issues.*,
      epic_title
    FROM {{ref('gitlab_dotcom_epic_issues')}} epic_issues
    LEFT JOIN {{ref('gitlab_dotcom_epics')}} epic
      ON epic_issues.epic_id = epic.epic_id
  
), joined AS (

  SELECT
    issues.issue_id,
    issues.issue_iid,
    issues.author_id,
    users.user_name                             AS author,
    users.notification_email                    AS author_email,
    users.is_external_user                      AS author_is_external_user,
    employees.division                          AS author_division,
    employees.department                        AS author_department,
    assignee.assignee,          
    assignee_email,
    employees_assignee.division                 AS assignee_division,
    employees_assignee.department               AS assignee_department,
    assignee_is_external_user,
    issues.project_id,
    milestone.milestone_title,
    epic.epic_title,
    updated_by_id,
    last_edited_by_id,
    moved_to_id,
    issues.created_at                            AS issue_created_at,
    issues.updated_at                            AS issue_updated_at,
    issue_last_edited_at,
    issue_closed_at,
    projects.namespace_id,
    visibility_level,
    is_confidential                              AS issue_is_confidential,
    
    CASE
      WHEN is_confidential = TRUE
        THEN 'confidential - masked'
      WHEN visibility_level != 'public'
        AND namespace_lineage.namespace_is_internal = FALSE
        THEN 'private/internal - masked'
      ELSE issue_title
    END                                          AS issue_title,
    
    CASE
      WHEN is_confidential = TRUE
        THEN 'confidential - masked'
      WHEN visibility_level != 'public'
        AND namespace_lineage.namespace_is_internal = FALSE
        THEN 'private/internal - masked'
      ELSE issue_description
    END                                          AS issue_description,
    

    CASE
    WHEN projects.namespace_id = 9970
      AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.labels)
      THEN TRUE
    ELSE FALSE
    END                                          AS is_community_contributor_related,

    CASE
      WHEN ARRAY_CONTAINS('s1'::variant, agg_labels.labels)
        THEN 'severity 1'
      WHEN ARRAY_CONTAINS('s2'::variant, agg_labels.labels)
        THEN 'severity 2'
      WHEN ARRAY_CONTAINS('s3'::variant, agg_labels.labels)
        THEN 'severity 3'
      WHEN ARRAY_CONTAINS('s4'::variant, agg_labels.labels)
        THEN 'severity 4'
      ELSE 'undefined'
    END                                          AS severity_tag,

    CASE
      WHEN ARRAY_CONTAINS('p1'::variant, agg_labels.labels) THEN 'priority 1'
      WHEN ARRAY_CONTAINS('p2'::variant, agg_labels.labels) THEN 'priority 2'
      WHEN ARRAY_CONTAINS('p3'::variant, agg_labels.labels) THEN 'priority 3'
      WHEN ARRAY_CONTAINS('p4'::variant, agg_labels.labels) THEN 'priority 4'
      ELSE 'undefined'
    END                                          AS priority_tag,

    CASE
      WHEN projects.namespace_id = 9970
        AND ARRAY_CONTAINS('security'::variant, agg_labels.labels)
        THEN TRUE
      ELSE FALSE
    END                                          AS is_security_issue,

    IFF(issues.project_id IN (13083, 278964, 250833, 7071551, 554859, 2009901, 2694799, 74823, 3674569, 430285, 5373222, 9762266, 5779760, 10476326, 9450197, 9450195, 9450192, 7145793, 6535935, 6280478, 6237091, 6237085, 5971589, 5606894, 4894834, 5851774, 5849726, 5778095, 5778093, 5778044, 5777976, 5777952, 5777939, 4157550, 1507906, 4382108, 8427052, 7524061, 7776928, 11520423, 10748426, 8368695, 5474112, 5467277, 10014839, 7660893, 6487905, 5777819, 145205, 3362933, 3430480, 4921652, 3626648, 6469877, 3601513, 12006272, 9049889, 734943, 14022, 1441932, 9396716, 6237088, 6126012, 6802300, 6143038, 5828233, 5777853, 8375261, 8368700, 5996549, 5727416, 5457755, 3828396, 7795571, 6392811, 6168240, 5778074, 5778037, 5777954, 5777179, 8362868, 9438583, 4949400, 5647182, 4359271, 4149988, 8226179, 11428501, 11432834, 7688358, 6328050, 5509547, 5456231, 11688089, 14240586, 11080193, 14694517, 6047528, 443787, 7750843, 4456656, 2670515, 6491770, 6130122, 11574953, 6388398, 5465687, 10619765, 9358979, 5778056, 5777918, 1075790, 3825482, 3787977, 11915984, 11317506, 10846938, 9712018, 6523803, 5737157, 5486671, 9184510, 20699, 10400718, 5457651, 27726, 6457868, 5467143, 10947578),
      TRUE, FALSE)                               AS is_included_in_engineering_metrics,
    IFF(issues.project_id IN (4359271, 7453181, 3828396, 6329679, 16590122, 13815397, 5279538, 10400718, 11688089, 9184510, 2670515, 150440, 13682597, 6047528, 4157550, 2694799, 36743, 1794617, 1507906, 13083, 145205, 5981322, 13764, 3626648, 3825482, 3601513, 14694517, 143237, 27726, 14378900, 10506825, 11196060, 3844141, 9450192, 6392811, 9762266, 12006272, 6237091, 5465687, 3101096, 1075790, 3787977, 928825, 13922331, 5498087, 5777819, 5777853, 5777952, 5777954, 5778074, 5778095, 13467157, 6280478, 5737157, 14097509, 5467143, 6469877, 6237088, 11432834, 5457755, 2651596, 2009901, 4534254, 74823, 1777822, 734943, 250833, 7792567, 6938270, 6457868, 20699, 13331704, 9358979, 5778056, 16683102, 11520423, 13664986, 11915984, 1254421, 278964, 5647182, 2953390, 554859, 11080193, 6299390, 8226179, 10973541, 3843116, 11015994, 10586771, 11380952, 15112583, 3362933, 10476326, 6487905, 9450197, 9450195, 10861561, 11139665, 5779760, 5777939, 8362868, 6126012, 5606894, 13150952, 5971589, 5727416, 5456231, 5486671, 6328050, 6523803, 10846938, 7688358, 16842968, 95156, 387896, 430285, 7795571, 13017938, 17035255, 17166102, 5777918, 5778037, 6237085, 6388398, 9712018, 8368700, 8368695, 5457651, 13831684, 14404642, 15667093, 15234689, 15642544, 8427052, 5509547, 6043225, 14022, 4949400, 7145793, 11625232, 10846951, 5777179, 5772881, 5777976, 5778093, 5828233, 6130122, 5373222, 11533294, 5996549, 5467277, 7071551, 15685887, 15683922, 7750843, 8375261, 10953870, 4875494, 3842996, 7660893, 5778044, 11373038, 13348998, 10748426, 5474112, 11317506, 6491770, 15704133, 15685912, 2959752, 11446522, 4456656, 6535935, 15685819, 9396716, 11574953, 9438583),
      TRUE, FALSE)                               AS is_part_of_product,
    state,
    weight,
    issues.due_date,
    lock_version,
    time_estimate,
    has_discussion_locked,
    closed_by_id,
    relative_position,
    service_desk_reply_to,
    duplicated_to_id,
    promoted_to_epic_id,

    agg_labels.labels,
    ARRAY_TO_STRING(agg_labels.labels,'|')       AS masked_label_title,

    namespace_lineage.namespace_is_internal      AS is_internal_issue,
    namespace_lineage.ultimate_parent_id,
    namespace_lineage.ultimate_parent_plan_id,
    namespace_lineage.ultimate_parent_plan_title,
    namespace_lineage.ultimate_parent_plan_is_paid

  FROM issues
  LEFT JOIN agg_labels
    ON issues.issue_id = agg_labels.issue_id
  LEFT JOIN projects
    ON issues.project_id = projects.project_id
  LEFT JOIN namespace_lineage
    ON projects.namespace_id = namespace_lineage.namespace_id
  LEFT JOIN users 
    ON issues.author_id = users.user_id
  LEFT JOIN assignee
    ON issues.issue_id = assignee.issue_id
  LEFT JOIN milestone
    ON milestone.milestone_id = issues.milestone_id
  LEFT JOIN epic
    ON epic.issue_id = issues.issue_id
  LEFT JOIN employees
    ON employees.work_email = users.notification_email
    AND employees.date_actual = TO_DATE(issues.created_at)
  LEFT JOIN employees AS employees_assignee
    ON employees_assignee.work_email = assignee.assignee_email
    AND employees.date_actual = COALESCE(issues.issue_closed_at,issues.updated_at)
)

SELECT *
FROM joined
WHERE is_internal_issue = True
