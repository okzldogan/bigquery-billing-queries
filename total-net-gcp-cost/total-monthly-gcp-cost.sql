SELECT
    PARSE_DATE("%Y%m", invoice.month) AS month,
    ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)),2) AS total_net_gcp_monthly_cost
FROM
    `MYPROJECT.MYDATASET.MYGCPBILLINGEXPORTRESOURCE`
GROUP BY
    month
ORDER BY
    month DESC
