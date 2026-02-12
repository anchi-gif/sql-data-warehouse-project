-- Issues from the CSV files  crm_cust_info 
/* 
===================================================================================
DDL Script: Create Bronze staging Tables
===================================================================================
There is an issue to import the CRM datasets in to MySQL without loosing data.
 To remediate, the datasets were imported to Python (using pandas) to replace the missing values by "None".
 This process permit to resolve the missing data issues in the bronze layers without modifying to much the data.
*/

-- Create table crm staging 
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info_stg(
cst_id              NVARCHAR(255),
cst_key             NVARCHAR(255),
cst_firstname       NVARCHAR(255),
cst_lastname        NVARCHAR(255),
cst_material_status NVARCHAR(255),
cst_gndr            NVARCHAR(255),
cst_create_date     NVARCHAR(255)
);

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info_stg(
prd_id        NVARCHAR(255),
prd_key       NVARCHAR(255),
prd_nm        NVARCHAR(255),
prd_cost      NVARCHAR(255),
prd_line      NVARCHAR(255),
prd_start_dt  NVARCHAR(255),
prd_end_dt    NVARCHAR(255)
);

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details_stg(
sls_ord_num    NVARCHAR(255),
sls_prd_key    NVARCHAR(255),
sls_cust_id    NVARCHAR(255),
sls_order_dt   NVARCHAR(255),
sls_ship_dt    NVARCHAR(255),
sls_due_dt     NVARCHAR(255),
sls_sales      NVARCHAR(255),
sls_quantity   NVARCHAR(255),
sls_price      NVARCHAR(255)
);

 -- Create ERP tables staging
 
CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101_stg(
cid   NVARCHAR(255),
cntry NVARCHAR(255)
);

CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12_stg(
cid   NVARCHAR(255),
bdate NVARCHAR(255),
gen   NVARCHAR(255)
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2_stg(
id          NVARCHAR(255),
cat         NVARCHAR(255),
subcat      NVARCHAR(255),
maintenance NVARCHAR(255)
);
