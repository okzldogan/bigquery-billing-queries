WITH january_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202401"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
february_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202402"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
march_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202403"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
april_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202404"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
may_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202405"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
june_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202406"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
july_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202407"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
),
august_2024_costs AS (
    SELECT 
        project.name AS project_name,
        ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS net_monthly_cost,
    FROM
        `DATASET_PROJECT.DATASET_NAME.gcp_billing_export_resource_v1_BILLING_ACCOUNT_ID`
    WHERE
        invoice.month = "202408"
    GROUP BY
        project_name
    ORDER BY
        net_monthly_cost DESC
    LIMIT
        40
)
SELECT
    january_2024_costs.project_name as project_name,
    january_2024_costs.net_monthly_cost as january_2024_cost,
    february_2024_costs.net_monthly_cost as february_2024_cost,
    march_2024_costs.net_monthly_cost as march_2024_cost,
    april_2024_costs.net_monthly_cost as april_2024_cost,
    may_2024_costs.net_monthly_cost as may_2024_cost,
    june_2024_costs.net_monthly_cost as june_2024_cost,
    july_2024_costs.net_monthly_cost as july_2024_cost,
	august_2024_costs.net_monthly_cost as august_2024_cost
FROM
    january_2024_costs
JOIN
    february_2024_costs
ON
    january_2024_costs.project_name = february_2024_costs.project_name
JOIN
    march_2024_costs
ON
    january_2024_costs.project_name = march_2024_costs.project_name
JOIN
    april_2024_costs
ON
    january_2024_costs.project_name = april_2024_costs.project_name
JOIN
    may_2024_costs
ON
    january_2024_costs.project_name = may_2024_costs.project_name
JOIN
    june_2024_costs
ON
    january_2024_costs.project_name = june_2024_costs.project_name
JOIN
    july_2024_costs
ON
    january_2024_costs.project_name = july_2024_costs.project_name
JOIN
    august_2024_costs
ON
    january_2024_costs.project_name = august_2024_costs.project_name
ORDER BY
    july_2024_cost DESC
