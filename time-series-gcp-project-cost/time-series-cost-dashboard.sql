WITH top_10_most_expensive_projects AS (
    SELECT
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `sp-billing-export.billing_export_dataset.gcp_billing_export_resource_v1_019B2C_BB4714_3D8191`
    WHERE
        EXTRACT(MONTH FROM usage_start_time) = EXTRACT(MONTH FROM CURRENT_DATE())
        AND cost > 0
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        10
)
SELECT
    -- Convert invoice.month to to timestamp
    PARSE_DATE("%Y%m", invoice.month) AS month,
    project.name AS project_name,
    -- (SELECT value from UNNEST(project.labels) WHERE key = "environment") as environment,
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
FROM
    `sp-billing-export.billing_export_dataset.gcp_billing_export_resource_v1_019B2C_BB4714_3D8191`
WHERE
    -- Pick top 10 project.name which have the highest net_monthly_costs
    project.name IN (SELECT project_name FROM top_10_most_expensive_projects)
GROUP BY
    project_name,
    month
ORDER BY
    net_monthly_cost DESC
LIMIT
    60