-- Check quality Gold Table

-- Checking data quality from dim_customers
SELECT *
FROM gold.dim_customers;

-- Checking gender column
SELECT DISTINCT gender
FROM gold.dim_customers;
--- ==================================================================
SELECT *
FROM gold.fact_sales;

-- Checking Foreign Key Integrity (Dimension)
SELECT *
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

SELECT *
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
WHERE c.product_key IS NULL;
