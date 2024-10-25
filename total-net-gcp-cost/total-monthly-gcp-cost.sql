SELECT
    PARSE_DATE("%Y%m", invoice.month) AS month,
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS total_net_gcp_monthly_cost
FROM
    `sp-billing-export.billing_export_dataset.gcp_billing_export_resource_v1_019B2C_BB4714_3D8191`
GROUP BY
    month
ORDER BY
    month DESC