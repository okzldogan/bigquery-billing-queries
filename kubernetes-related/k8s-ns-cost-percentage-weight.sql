WITH k8s_namespace_costs AS (
    SELECT
    project.name AS project_name,
    -- Sum up kubernetes related namespace into "kubernetes-overhead"
    IF(labels.value = "kube-system" OR labels.value = "kube:unallocated" OR labels.value = "goog-k8s-unknown" OR
    labels.value = "kube:system-overhead" OR labels.value = "goog-k8s-unsupported-sku" OR labels.value IS NULL, "kubernetes-overhead", labels.value) AS namespace,
    -- Deduct the credits from the cost
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS cost_after_credits,
    FORMAT_DATE("%b-%Y", DATE(usage_start_time)) AS month,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    LEFT JOIN UNNEST(labels) as labels
        ON labels.key = "k8s-namespace"
    WHERE
        project.name = "GKE_HOSTING_PROJECT_NAME"
        AND EXTRACT(MONTH FROM usage_start_time) = 05
    GROUP BY 
        namespace,
        month,
        project_name
    ORDER BY
        cost_after_credits DESC
),
application_namespaces AS (
    SELECT
        project_name,
        namespace,
        cost_after_credits,
    month
    FROM k8s_namespace_costs
    WHERE 
        namespace != "kubernetes-overhead"
),
kubernetes_overhead_cost AS (
    SELECT
        namespace,
        cost_after_credits as overhead_cost,
    FROM k8s_namespace_costs
    WHERE 
        namespace = "kubernetes-overhead"
)
SELECT
    application_namespaces.month as month,
    application_namespaces.project_name as project_name,
    application_namespaces.namespace as namespace,
    application_namespaces.cost_after_credits as cost_after_credits,
    -- Get average cost percentage weight
    ROUND((ROUND((application_namespaces.cost_after_credits / SUM(application_namespaces.cost_after_credits) OVER (PARTITION BY month)), 6) * kubernetes_overhead_cost.overhead_cost), 2) as weighted_overhead_distribution,
    ROUND((ROUND((ROUND((application_namespaces.cost_after_credits / SUM(application_namespaces.cost_after_credits) OVER (PARTITION BY month)), 6) * kubernetes_overhead_cost.overhead_cost), 2) + application_namespaces.cost_after_credits), 2) as namespace_cost
FROM 
    application_namespaces,
    kubernetes_overhead_cost
GROUP BY
    project_name,
    namespace,
    cost_after_credits,
    month,
    kubernetes_overhead_cost.overhead_cost
ORDER BY
    weighted_overhead_distribution DESC