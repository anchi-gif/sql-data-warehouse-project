/*
===================================================================================
Stored Procedure: Load Bronze Layer (staging tables -> Bronze)
===================================================================================
Purpose:
	This stored procedure transfers data from staging tables into Bronze tables.
Key Features:
	- Truncates target Bronze tables before loading.
    - Insert data from staging tables.
    - Uses transaction management (COMMIT / ROLLBACK).
    - Implement error handling.
    - Logs execution details into bronze.load_audit.

To ensure controlled, auditable, and repeatable data loads.

Parameters:
	None.
    This stored procedure does not accept any parameters or return any values.
    
Usage Example:
	CALL load_bronze();
*/

DELIMITER //
DROP PROCEDURE IF EXISTS load_bronze;
CREATE PROCEDURE load_bronze()
BEGIN
	-- Declaring variables(v)
	DECLARE v_start_time DATETIME;
  DECLARE v_end_time DATETIME;
  DECLARE v_duration INT;
  DECLARE v_status   VARCHAR(50);
  DECLARE v_error    TEXT;
    
-- Error handler
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
      SET v_status = 'FAILED';
      SET v_error = 'Error occured during Bronze load';
      SET v_end_time = NOW();
      SET v_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time);
		  SELECT 'An error occured. Rolling back.' AS error_message;

      ROLLBACK;
        
	    INSERT INTO bronze.load_audit
      (procedure_name, layer_name, start_time, end_time, duration_seconds, status, error_message)
      VALUES
      ('load_bronze', 'bronze', v_start_time, v_end_time, v_duration, v_status, v_error);
	END;
  -- Start tracking
  SET v_start_time = NOW();
  SET v_status = 'RUNNING';
	SET v_error = NULL;
    
  START TRANSACTION;

-- '============================================';
--  'Loading Bronze Layer' ;
-- '============================================';
    
-- '--------------------------------------------';
-- 'Loading CRM Tables';
-- '--------------------------------------------';
    
-- customers table
	-- '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
    
    -- '>> Inserting Data Into: bronze.crm_cust_info';
	INSERT INTO bronze.crm_cust_info
	SELECT * FROM bronze.crm_cust_info_stg;

-- Products 
	-- '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
    
    -- '>> Inserting Data Into: bronze.crm_prd_info';
	INSERT INTO bronze.crm_prd_info
	SELECT * FROM bronze.crm_prd_info_stg;

-- Sales
	-- '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
    
    -- '>> Inserting Data Into: bronze.crm_sales_details';
	INSERT INTO bronze.crm_sales_details
	SELECT * FROM bronze.crm_sales_details_stg;
	
    -- '--------------------------------------------';
	--  'Loading ERP Tables';
    -- '--------------------------------------------';
    
-- erp_cust 
	-- '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
    
-- SELECT'>> Inserting Data Into: bronze.erp_cust_az12';
	INSERT INTO bronze.erp_cust_az12
	SELECT * FROM bronze.erp_cust_az12_stg;

-- erp_loc 
	-- '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
    
    -- '>> Inserting Data Into: bronze.erp_loc_a101';
	INSERT INTO bronze.erp_loc_a101
	SELECT * FROM bronze.erp_loc_a101_stg;

-- erp_px_cat
	-- '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    -- '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	INSERT INTO bronze.erp_px_cat_g1v2
	SELECT * FROM bronze.erp_px_cat_g1v2_stg;
	
    COMMIT;
    
    SET v_status = 'SUCCESS';
    SET v_end_time = NOW();
    SET v_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time);
    
    INSERT INTO bronze.load_audit
	  (procedure_name, layer_name, start_time, end_time, duration_seconds, status, error_message)
	  VALUES
	  ('load_bronze', 'bronze', v_start_time, v_end_time, v_duration, v_status, v_error);
	
    SELECT 'Bronze Layer Loaded Successfully' AS status;
END //
DELIMITER ;


