/*
===================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===================================================================================
Purpose:
	This stored procedure performs ETL (Extract, Transform, Load) process to transfer 
data from Bronze to Silver.

Key Features:
  - Truncates Silver Tables
  - Inserts transformed and cleansed data from Bronze into Silver Tables

Parameters:
	None.
    This stored procedure does not accept any parameters or return any values.
    
Usage Example:
	CALL load_silver();
then
 SELECT *
FROM silver.load_audit
ORDER BY audit_id DESC;
*/

DELIMITER //
DROP PROCEDURE IF EXISTS load_silver;
CREATE PROCEDURE load_silver()
BEGIN
	-- Declaring variables(v)
	DECLARE v_start_time DATETIME;
    DECLARE v_end_time DATETIME;
    DECLARE v_duration INT;
    DECLARE v_status VARCHAR(50);
    DECLARE v_error TEXT;
    
	-- Error handler
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
		SET v_status = 'FAILED';
        SET v_error = 'Error occured during Silver load';
        SET v_end_time = NOW();
        SET v_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time);
		SELECT 'An error occured. Rolling back.' AS error_message;
        
        ROLLBACK;
        
        INSERT INTO silver.load_audit
        (procedure_name, layer_name, start_time, end_time, duration_seconds, status, error_message)
        VALUES
        ('load_silver', 'silver', v_start_time, v_end_time, v_duration, v_status, v_error);
        
	END ;
    
    -- Start tracking
    SET v_start_time = NOW();
    SET v_status = 'RUNNING';
	  SET v_error = NULL;
    
	  START TRANSACTION;
    
-- ===================================================================================
-- 'CRM Tables'
-- ===================================================================================
    
	  -- '>>Truncating Table: silver.crm_cust_info'
	  TRUNCATE TABLE silver.crm_cust_info;

	  -- '>>Inserting Data Into: silver.crm_cust_info '
	  INSERT INTO silver.crm_cust_info(
  		cst_id,
  		cst_key,
  		cst_firstname,
  		cst_lastname,
  		cst_marital_status,
  		cst_gndr,
  		cst_create_date
	)
	  SELECT 
  		cst_id,
  		cst_key,
      TRIM(cst_firstname) AS cst_firstname,
  	  TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    		 ELSE 'n/a'
   END AS cst_marital_status,  
	 CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		    ELSE 'n/a'
	 END AS cst_gndr, 
	 cst_create_date
	 FROM (
  		SELECT *,
  		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
  		FROM bronze.crm_cust_info
  		WHERE cst_id IS NOT NULL
  	 )t WHERE flag_last = 1; 

	 -- '>>Truncating Table: silver.crm_prd_info'
	 TRUNCATE TABLE silver.crm_prd_info;

	 -- '>>Inserting Data Into: silver.crm_prd_info'
	 INSERT INTO silver.crm_prd_info(
		prd_id,
		prd_cat,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	 )
  SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, CHAR_LENGTH(prd_key)) AS prd_key, 
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
  		  WHEN  'M' THEN 'Mountain'
      	WHEN  'R' THEN 'Road'
      	WHEN  'S' THEN 'Other Sales'
      	WHEN  'T' THEN 'Touring'
      	ELSE 'n/a'
  	END AS prd_line,
  	CAST(prd_start_dt AS DATE) AS prd_start_dt,
  	CAST(DATE_SUB(LEAD(prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY ) AS DATE) AS prd_end_dt
  	FROM bronze.crm_prd_info;

  	-- '>> Truncating Table: silver.crm_sales_details'
  	TRUNCATE TABLE silver.crm_sales_details;
  
  	-- '>> Inserting Data Into: silver.crm_sales_details'
  	INSERT INTO silver.crm_sales_details(
  		sls_ord_num,
  		sls_prd_key,
  		sls_cust_id,
  		sls_order_dt,
  		sls_ship_dt,
  		sls_due_dt,
  		sls_sales,
  		sls_quantity,
  		sls_price
  	)
  	SELECT
  	sls_ord_num,
  	sls_prd_key,
  	sls_cust_id,
  	CASE
  		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL 
  		ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
  	END AS sls_order_dt,
  	CASE
  		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL 
  		ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
  	END AS sls_ship_dt,
  	CASE
  		WHEN sls_due_dt = 0 OR CHAR_LENGTH(CAST(sls_due_dt AS CHAR)) != 8 THEN NULL 
  		ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
  	END AS sls_due_dt,
  	CASE
  		WHEN sls_sales IS NULL OR sls_price <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
  		ELSE sls_sales
  	END AS sls_sales,
  	sls_quantity,
  	CASE 
  		WHEN sls_price IS NULL OR sls_price <= 0 
  		THEN ROUND(sls_sales / NULLIF(sls_quantity, 0),0)
  	ELSE sls_price
  	END AS sls_price
  	FROM bronze.crm_sales_details;
        
-- ===================================================================================
-- 'ERP Tables' 
-- ===================================================================================
    
	-- 'Truncating Table: silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12;

	-- 'Inserting Data Into: silver.erp_cust_az12'
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)

	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid,
	CASE WHEN bdate > CURDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;

	-- '>> Truncating Table: silver.erp_loc_a101'
	TRUNCATE TABLE silver.erp_loc_a101;

	-- '>> Inserting Data Into: silver.erp_loc_a101'
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT 
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101; 

	-- '>> Truncating Table: silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	-- '>> Inserting Data Into: silver.erp_px_cat_g1v2'
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
    
  COMMIT;
    
  SET v_status = 'SUCCESS';
  SET v_end_time = NOW();
  SET v_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time);
    
  INSERT INTO silver.load_audit
	(procedure_name, layer_name, start_time, end_time, duration_seconds, status, error_message)
	VALUES
	('load_silver', 'silver', v_start_time, v_end_time, v_duration, v_status, v_error);
	
  SELECT 'Silver Layer Inserted Successfully' AS status;
	
END // 
DELIMITER ;
