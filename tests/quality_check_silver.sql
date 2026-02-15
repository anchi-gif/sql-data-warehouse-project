/*
============================================================================================
Quality Checks.
============================================================================================
Script Purpose:
  In this script various quality checks were done for data consistency, accuracy, and 
standardization across the 'silver' layer.

Check for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Invalid date ranges and orders.
  - Data standardization and consistency.
  - Data consistency between related fields.

Usage Notes:
  - Run these queries to check after data loading in the Silver Layer.
  - Can add more query if needed .
*/


-- ============================================================================================
-- Checking for silver.crm_cust_info
-- ============================================================================================
-- Check for unwanted spaces in string values
-- Expectation: No Result
SELECT cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT *
FROM silver.crm_cust_info;

-- ============================================================================================
-- crm_prd_info
-- ============================================================================================
-- Check for Nulls or duplicate in Primary Key
-- Expectation: No Result

SELECT prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Check for unwanted spaces in string values
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- Checks for Nulls and negative numbers 
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check consistency of values in low cardinality columns(gndr, marital_status)
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Order 
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT *
FROM silver.crm_prd_info;

-- ============================================================================================
-- crm_sales_details
-- ============================================================================================
-- Checking date from INT to Date type
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8;

-- Checking for invalid date order (oder date < ship date)
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT *
FROM silver.crm_sales_details;


-- ============================================================================================
-- erp_cust_az12
-- ============================================================================================
-- checking bdate if not out of range --> replace the extreme date
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURDATE();

-- SILVER
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURDATE();

SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT *
FROM silver.erp_cust_az12;

-- ============================================================================================
-- erp_loc_a101
-- ============================================================================================
-- Data standardization & Consistency

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT * FROM bronze.erp_loc_a101;

-- ============================================================================================
-- erp_px_cat_g1v2
-- ============================================================================================
SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

SELECT *
FROM silver.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

SELECT * FROM silver.erp_px_cat_g1v2;
