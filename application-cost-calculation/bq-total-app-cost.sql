WITH project_costs AS (
    SELECT
    (SELECT value from UNNEST(project.labels) where key = "environment") AS environment,
    project.name AS project_name,
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS cost_after_credits,
    FORMAT_DATE("%b-%Y", DATE(usage_start_time)) AS month
FROM
    `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
WHERE
    project.name LIKE "%MY_PROJECT%"
    AND EXTRACT(MONTH FROM usage_start_time) = 12
    AND cost > 0
GROUP BY
    project_name,
    month,
    environment
ORDER BY
    cost_after_credits DESC
),
kubernetes_namespace_costs AS (
    SELECT
        (SELECT value from UNNEST(project.labels) where key = "environment") AS environment,
        labels.value AS namespace,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS cost_after_credits,
        FORMAT_DATE("%b-%Y", DATE(usage_start_time)) AS month
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    LEFT JOIN UNNEST(labels) as labels
        ON labels.key = "k8s-namespace"
    WHERE
        labels.value = "MY_PROJECT"
        AND EXTRACT(MONTH FROM usage_start_time) = 12
    GROUP BY
        namespace,
        month,
        environment
),
kubernetes_allocation_costs AS (
    SELECT
        project.name AS project_name,
        -- Sum up kubernetes related namespace into "kubernetes-overhead"
        IF(labels.value = "kube-system" OR labels.value = "kube:unallocated" OR labels.value = "goog-k8s-unknown" OR
        labels.value = "kube:system-overhead" OR labels.value = "goog-k8s-unsupported-sku" OR labels.value IS NULL, "kubernetes-overhead", labels.value) AS namespace,
        -- Deduct the credits from the cost
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS cost_after_credits,
        FORMAT_DATE("%b-%Y", DATE(usage_start_time)) AS month,
        (SELECT value from UNNEST(project.labels) where key = "environment") AS environment,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    LEFT JOIN UNNEST(labels) as labels
        ON labels.key = "k8s-namespace"
    WHERE
        project.name LIKE "%GKE_HOSTING_PROJECT%"
        AND EXTRACT(MONTH FROM usage_start_time) = 12
    GROUP BY 
        namespace,
        month,
        project_name,
        environment
),
kubernetes_overhead_costs AS (
    SELECT
        kubernetes_allocation_costs.project_name as project_name,
        kubernetes_allocation_costs.namespace,
        kubernetes_allocation_costs.cost_after_credits as overhead_costs,
        kubernetes_allocation_costs.month,
        kubernetes_allocation_costs.environment as environment
    FROM
        kubernetes_allocation_costs
    WHERE
        kubernetes_allocation_costs.project_name LIKE "%GKE_HOSTING_PROJECT%"
        AND kubernetes_allocation_costs.namespace = "kubernetes-overhead"
),
namespace_count_per_cluster AS (
    SELECT
        COUNT(DISTINCT kubernetes_allocation_costs.namespace) AS namespace_count,
        kubernetes_allocation_costs.environment as environment,
    FROM
        kubernetes_allocation_costs
    WHERE
        kubernetes_allocation_costs.project_name LIKE "%GKE_HOSTING_PROJECT%"
        AND kubernetes_allocation_costs.namespace != "kubernetes-overhead"
    GROUP BY
        kubernetes_allocation_costs.environment
),
joined_application_costs AS (
    SELECT
        project_costs.cost_after_credits as project_cost,
        kubernetes_namespace_costs.cost_after_credits as namespace_cost,
        project_costs.environment as project_environment,
        project_costs.month as project_invoice_month,
        kubernetes_namespace_costs.environment,
        kubernetes_namespace_costs.month
    FROM
        project_costs,
        kubernetes_namespace_costs
    WHERE
        project_costs.month = kubernetes_namespace_costs.month
        AND project_costs.environment = kubernetes_namespace_costs.environment
)
SELECT
    ROUND(SUM(joined_application_costs.project_cost+joined_application_costs.namespace_cost) + (kubernetes_overhead_costs.overhead_costs / namespace_count_per_cluster.namespace_count),2) AS total_cost_for_the_app,
    kubernetes_allocation_costs.namespace as namespace,
    joined_application_costs.project_environment as environment,
    ROUND(SUM(joined_application_costs.project_cost+joined_application_costs.namespace_cost),2) AS project_and_namespace_cost,
    joined_application_costs.project_invoice_month as month,
    kubernetes_overhead_costs.overhead_costs as gke_overhead_costs,
    namespace_count_per_cluster.namespace_count as namespace_count,
    ROUND((kubernetes_overhead_costs.overhead_costs / namespace_count_per_cluster.namespace_count),2) as overhead_per_namespace
FROM
    joined_application_costs,
    kubernetes_overhead_costs,
    namespace_count_per_cluster,
    kubernetes_allocation_costs
WHERE
    joined_application_costs.environment = namespace_count_per_cluster.environment
    AND joined_application_costs.environment = kubernetes_overhead_costs.environment
    AND kubernetes_allocation_costs.namespace = "MY_PROJECT"
GROUP BY
    joined_application_costs.project_environment,
    joined_application_costs.project_invoice_month,
    namespace_count_per_cluster.namespace_count,
    kubernetes_overhead_costs.overhead_costs,
    kubernetes_overhead_costs.overhead_costs / namespace_count_per_cluster.namespace_count,
    kubernetes_allocation_costs.namespace
ORDER BY
    total_cost_for_the_app DESC
