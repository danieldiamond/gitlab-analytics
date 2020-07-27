{{ config(materialized='view') }}

WITH namespace_snapshots_monthly AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_snapshots_monthly') }}

), recursive_namespace_ultimate(snapshot_month, namespace_id, parent_id, upstream_lineage) AS (
    
   SELECT
     snapshot_month,
     namespace_snapshots_monthly.namespace_id,
     namespace_snapshots_monthly.parent_id,
     TO_ARRAY(namespace_id)                                      AS upstream_lineage
   FROM namespace_snapshots_monthly
   WHERE parent_id IS NULL
  
   UNION ALL
  
   SELECT
     iter.snapshot_month,
     iter.namespace_id,
     iter.parent_id,
     ARRAY_INSERT(anchor.upstream_lineage, 0, iter.namespace_id)  AS upstream_lineage
   FROM recursive_namespace_ultimate AS anchor
   INNER JOIN namespace_snapshots_monthly AS iter
     ON iter.parent_id = anchor.namespace_id
     AND iter.snapshot_month = anchor.snapshot_month
  
), namespace_lineage_monthly AS (
    
    SELECT
      recursive_namespace_ultimate.*,
      upstream_lineage[ARRAY_SIZE(upstream_lineage) - 1] AS ultimate_parent_id,
      COALESCE((ultimate_parent_id IN {{ get_internal_parent_namespaces() }}), FALSE)   AS namespace_is_internal
    FROM recursive_namespace_ultimate
)

SELECT *
FROM namespace_lineage_monthly
