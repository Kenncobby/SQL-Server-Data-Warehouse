/*
=============================================================================================
Power BI Reconciliation Baseline
=============================================================================================
Script Purpose:
	Produces the SQL-source-of-truth figures that every Power BI measure in the semantic
	model must match exactly (see docs/powerbi_reconciliation_results.md and
	docs/powerbi_measures.md). Run against gold.* views only, since those are the tables
	Power BI imports from.

Notes:
	- 19 sales lines have an invalid/blank order_date (nulled out in Silver); they are
	  included in the "All data" totals but fall out of the "by calendar year" grouping
	  under a NULL year bucket, shown separately below.
=============================================================================================
*/

-- ============================================================
-- 1. Total sales, quantity, distinct orders/customers, line count - ALL DATA
-- ============================================================
SELECT
	SUM(sales_amount)                    AS total_sales,
	SUM(quantity)                        AS total_quantity,
	COUNT(DISTINCT order_number)         AS distinct_orders,
	COUNT(DISTINCT customer_key)         AS distinct_customers_with_sales,
	COUNT(*)                             AS line_count
FROM gold.fact_sales;

-- ============================================================
-- 2. Same metrics, BY CALENDAR YEAR (NULL = 19 lines with invalid order_date)
-- ============================================================
SELECT
	YEAR(order_date)                     AS order_year,
	SUM(sales_amount)                    AS total_sales,
	SUM(quantity)                        AS total_quantity,
	COUNT(DISTINCT order_number)         AS distinct_orders,
	COUNT(DISTINCT customer_key)         AS distinct_customers_with_sales,
	COUNT(*)                             AS line_count
FROM gold.fact_sales
GROUP BY YEAR(order_date)
ORDER BY order_year;

-- ============================================================
-- 3. Total sales by category
-- ============================================================
SELECT
	ISNULL(p.category, 'Unknown')        AS category,
	SUM(f.sales_amount)                  AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_sales DESC;

-- ============================================================
-- 4. Total sales by country
-- ============================================================
SELECT
	ISNULL(c.country, 'Unknown')         AS country,
	SUM(f.sales_amount)                  AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;

-- ============================================================
-- 5. Total cost = SUM(quantity * cost), joined to dim_products
--    (orphan lines with no product_key contribute 0 cost)
-- ============================================================
SELECT
	SUM(f.quantity * p.cost)             AS total_cost
FROM gold.fact_sales f
JOIN gold.dim_products p ON p.product_key = f.product_key;

-- ============================================================
-- 6. Average days to ship
-- ============================================================
SELECT
	AVG(DATEDIFF(day, order_date, shipping_date) * 1.0) AS avg_days_to_ship
FROM gold.fact_sales;

-- ============================================================
-- 7. Share of lines shipped on or before the due date
-- ============================================================
SELECT
	CAST(SUM(CASE WHEN shipping_date <= due_date THEN 1 ELSE 0 END) AS FLOAT)
		/ COUNT(*)                       AS on_time_ship_share
FROM gold.fact_sales;

-- ============================================================
-- 8. New customers baseline: customers whose FIRST order falls in each year
-- ============================================================
WITH first_orders AS (
	SELECT
		customer_key,
		MIN(order_date) AS first_order_date
	FROM gold.fact_sales
	WHERE customer_key IS NOT NULL AND order_date IS NOT NULL
	GROUP BY customer_key
)
SELECT
	YEAR(first_order_date)               AS cohort_year,
	COUNT(*)                             AS new_customers
FROM first_orders
GROUP BY YEAR(first_order_date)
ORDER BY cohort_year;
