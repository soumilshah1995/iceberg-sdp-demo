-- Silver. Recount every raw customer into age groups.
-- This is a materialized view, so each run replaces these totals.
-- The dev catalog is Iceberg, so this table is stored as Iceberg.

CREATE MATERIALIZED VIEW customers_by_age_group
AS
SELECT
  age_group,
  COUNT(*) AS customer_count,
  AVG(age) AS avg_age,
  MIN(age) AS youngest,
  MAX(age) AS oldest
FROM (
  SELECT
    CAST(age AS INT) AS age,
    CASE
      WHEN CAST(age AS INT) < 30 THEN 'under_30'
      WHEN CAST(age AS INT) < 60 THEN '30_to_59'
      ELSE '60_plus'
    END AS age_group
  FROM raw_customers
)
GROUP BY age_group;
