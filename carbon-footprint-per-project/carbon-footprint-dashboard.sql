WITH emissions_january_2024 AS (
  SELECT
    FORMAT_DATE("%b-%Y", DATE(usage_month)) AS month,
    project.id AS project_name,
    SUM(carbon_footprint_total_kgCO2e.location_based) AS total_carbon_footprint_per_project_location_based
  FROM
    `DATASET_PROJECT.DATASET_NAME.carbon_footprint`
  WHERE
    EXTRACT(MONTH FROM usage_month) = 01
    AND EXTRACT(YEAR FROM usage_month) = 2024
  GROUP BY
    month,
    project_name
),
emissions_december_2023 AS (
  SELECT
    FORMAT_DATE("%b-%Y", DATE(usage_month)) AS month,
    project.id AS project_name,
    SUM(carbon_footprint_total_kgCO2e.location_based) AS total_carbon_footprint_per_project_location_based
  FROM
    `DATASET_PROJECT.DATASET_NAME.carbon_footprint`
  WHERE
    EXTRACT(MONTH FROM usage_month) = 12
    AND EXTRACT(YEAR FROM usage_month) = 2023
  GROUP BY
    month,
    project_name
),
emissions_february_2024 AS (
  SELECT
    FORMAT_DATE("%b-%Y", DATE(usage_month)) AS month,
    project.id AS project_name,
    SUM(carbon_footprint_total_kgCO2e.location_based) AS total_carbon_footprint_per_project_location_based
  FROM
    `DATASET_PROJECT.DATASET_NAME.carbon_footprint`
  WHERE
    EXTRACT(MONTH FROM usage_month) = 02
    AND EXTRACT(YEAR FROM usage_month) = 2024
  GROUP BY
    month,
    project_name
),
emissions_march_2024 AS (
  SELECT
    FORMAT_DATE("%b-%Y", DATE(usage_month)) AS month,
    project.id AS project_name,
    SUM(carbon_footprint_total_kgCO2e.location_based) AS total_carbon_footprint_per_project_location_based
  FROM
    `DATASET_PROJECT.DATASET_NAME.carbon_footprint`
  WHERE
    EXTRACT(MONTH FROM usage_month) = 03
    AND EXTRACT(YEAR FROM usage_month) = 2024
  GROUP BY
    month,
    project_name
),
emissions_april_2024 AS (
  SELECT
    FORMAT_DATE("%b-%Y", DATE(usage_month)) AS month,
    project.id AS project_name,
    SUM(carbon_footprint_total_kgCO2e.location_based) AS total_carbon_footprint_per_project_location_based
  FROM
    `DATASET_PROJECT.DATASET_NAME.carbon_footprint`
  WHERE
    EXTRACT(MONTH FROM usage_month) = 04
    AND EXTRACT(YEAR FROM usage_month) = 2024
  GROUP BY
    month,
    project_name
),
emissions_may_2024 AS (
  SELECT
    FORMAT_DATE("%b-%Y", DATE(usage_month)) AS month,
    project.id AS project_name,
    SUM(carbon_footprint_total_kgCO2e.location_based) AS total_carbon_footprint_per_project_location_based
  FROM
    `DATASET_PROJECT.DATASET_NAME.carbon_footprint`
  WHERE
    EXTRACT(MONTH FROM usage_month) = 05
    AND EXTRACT(YEAR FROM usage_month) = 2024
  GROUP BY
    month,
    project_name
)
SELECT
    emissions_january_2024.project_name as project_name,
    emissions_december_2023.total_carbon_footprint_per_project_location_based as december_2023,
    emissions_january_2024.total_carbon_footprint_per_project_location_based as january_2024,
    emissions_february_2024.total_carbon_footprint_per_project_location_based as february_2024,
    emissions_march_2024.total_carbon_footprint_per_project_location_based as march_2024,
    emissions_april_2024.total_carbon_footprint_per_project_location_based as april_2024,
    emissions_may_2024.total_carbon_footprint_per_project_location_based as may_2024
FROM
    emissions_january_2024
JOIN
    emissions_december_2023
ON
    emissions_january_2024.project_name = emissions_december_2023.project_name
JOIN
    emissions_february_2024
ON
    emissions_january_2024.project_name = emissions_february_2024.project_name
JOIN
    emissions_march_2024
ON
    emissions_january_2024.project_name = emissions_march_2024.project_name
JOIN
    emissions_april_2024
ON
    emissions_january_2024.project_name = emissions_april_2024.project_name
JOIN
    emissions_may_2024
ON
    emissions_january_2024.project_name = emissions_may_2024.project_name
ORDER BY
    april_2024 DESC