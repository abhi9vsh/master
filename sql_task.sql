-- Databricks notebook source
WITH distinct_dates AS (
    SELECT DISTINCT redemptionDate
    FROM `[project].[dataset].tblRedemptions-ByDay` rd
    WHERE redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
),
LatestRedemptions AS (
    SELECT
        r.retailerName,
        rd.redemptionDate,
        rd.redemptionCount,
        rd.createDateTime,
        ROW_NUMBER() OVER (PARTITION BY rd.redemptionDate ORDER BY rd.createDateTime DESC) AS rn
    FROM
        `[project].[dataset].tblRedemptions-ByDay` rd
    JOIN
        `[project].[dataset].tblRetailers` r
    ON
        rd.retailerId = r.id
    WHERE
        r.retailerName = 'ABC Store'
        AND rd.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
)
SELECT
    dd.redemptionDate,
    COALESCE(lr.redemptionCount, 0) AS redemptionCount
FROM
    distinct_dates dd
LEFT JOIN (
    SELECT
        redemptionDate,
        redemptionCount
    FROM
        LatestRedemptions
    WHERE
        rn = 1
) lr
ON
    dd.redemptionDate = lr.redemptionDate
ORDER BY
    dd.redemptionDate;

-- COMMAND ----------

-- MAGIC %md Questions and Answers

-- COMMAND ----------

1. least number of redemptions are 05-11-2023 as 3702
2. Most number of redemptions are on 04-11-2023 as 5224
3. for least - 2023-11-06 11:00:00 UTC, for highest - 2023-11-05 11:00:00 UTC
4. We can achieve a similar result using GROUP BY without needing the ROW_NUMBER() window function or a CTE for distinct dates. Instead, we can group by redemptionDate and use MAX(createDateTime) to find the latest redemption count for each date.
