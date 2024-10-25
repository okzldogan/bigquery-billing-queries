SELECT
    project.name AS project_name,
    -- Sum up kubernetes related namespace into "kubernetes-overhead"
    IF(labels.value = "kube-system" OR labels.value = "kube:unallocated" OR labels.value = "goog-k8s-unknown" OR
    labels.value = "kube:system-overhead" OR labels.value = "goog-k8s-unsupported-sku" OR labels.value IS NULL, "kubernetes-overhead", labels.value) AS namespace,
    -- Deduct the credits from the cost
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS cost_after_credits,
    FORMAT_DATE("%b-%Y", DATE(usage_start_time)) AS month,
    -- Calculate Weighted kubernetes overhead cost by adding all the rows except for the "kubernetes-overhead" row and use the sum to divide the cost_after_credits
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) / SUM(IF(labels.value != "kubernetes-overhead", cost, 0)),2) AS weighted_kubernetes_overhead_cost
FROM
    `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
LEFT JOIN UNNEST(labels) as labels
    ON labels.key = "k8s-namespace"
WHERE
    project.name = "GKE_HOSTING_PROJECT"
    AND EXTRACT(MONTH FROM usage_start_time) = 05
GROUP BY 
    namespace,
    month,
    project_name
ORDER BY
    cost_after_credits DESC