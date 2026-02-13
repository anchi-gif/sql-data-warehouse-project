# %%
"""
File: Load_staging_with_python.py

Purpose:
    This script loads raw CSV data into Bronze staging tables using MySQL Connector and Python.

Steps:
    1. Read CSV file using pandas.
    2. Replace NaN values with None.
    3. Insert data into staging tables.
    4. Commit transaction.
This script is part of the Bronze layer ingestion process.
"""

# %%
import pandas as pd

# %% [markdown]
# # Importing CRM datafiles

# %%
df_cust= pd.read_csv("...sql-data-warehouse-project\\datasets\\source_crm\\cust_info.csv",
                 sep ="," ,
                 engine = "python",
                 dtype = str)               

# %% [markdown]
# ### Replacing NaN values by None

# %%
df_cust = df_cust.where(pd.notnull(df_cust), None)

# %%
df_prd = pd.read_csv("...sql-data-warehouse-project\\datasets\\source_crm\\prd_info.csv",
                 sep ="," ,
                 engine = "python",
                 dtype = str)  

# %%
df_prd

# %% [markdown]
# ### Replacing nan values by None

# %%
df_prd = df_prd.where(pd.notnull(df_prd), None)

# %%
df_prd

# %%
df_sales = pd.read_csv("...\\sql-data-warehouse-project\\datasets\\source_crm\\sales_details.csv",
                 sep ="," ,
                 engine = "python",
                 dtype = str)

# %% [markdown]
# ### Replacing NaN values by None

# %%
df_sales = df_sales.where(pd.notnull(df_sales), None)

# %% [markdown]
# ## Installing connector

# %%
!pip install mysql-connector-python

# %%
import os

conn = mysql.connector.connect(
    host = "localhost",
    user ="etl",
    password = "MYSQL_PASSWORD",
    database ="bronze"
)

print("Connection successful")

# %% [markdown]
# ## Staging crm_cust_info_stg table 

# %%

cursor = conn.cursor()
insert_sql = """
INSERT INTO crm_cust_info_stg(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status ,
    cst_gndr,
    cst_create_date
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
for _, row in df_cust.iterrows():
    cursor.execute(insert_sql, tuple(row))
conn.commit()

print("Insert completed")

# %% [markdown]
# ### Staging crm_prd_info_stag table 

# %%
cursor = conn.cursor()
insert_sql = """
INSERT INTO crm_prd_info_stg(
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
for _, row in df_prd.iterrows():
    cursor.execute(insert_sql, tuple(row))
conn.commit()

print("Insert completed")

# %% [markdown]
# ## Staging crm_sales_details_stg table 

# %%
cursor = conn.cursor()
insert_sql = """
INSERT INTO crm_sales_details_stg(
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
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for _, row in df_sales.iterrows():
    cursor.execute(insert_sql, tuple(row))
conn.commit()

print("Insert completed")

# %% [markdown]
# # Importing ERP datafiles

# %%
df_cust_az12= pd.read_csv("...\\sql-data-warehouse-project\\datasets\\source_erp\\CUST_AZ12.csv",
                 sep ="," ,
                 engine = "python",
                 dtype = str) 

# %%
df_loc_a101 = pd.read_csv("...\\sql-data-warehouse-project\\datasets\\source_erp\\LOC_A101.csv",
                 sep ="," ,
                 engine = "python",
                 dtype = str) 

# %%
df_px_cat = pd.read_csv("...\\sql-data-warehouse-project\\datasets\\source_erp\\PX_CAT_G1V2.csv",
                 sep ="," ,
                 engine = "python",
                 dtype = str) 

# %% [markdown]
# ### Replacing NaN values by None

# %%
### erp_cust_az12 table
df_cust_az12 = df_cust_az12.where(pd.notnull(df_cust_az12), None)

### erp_loc_a101 table
df_loc_a101 = df_loc_a101.where(pd.notnull(df_loc_a101), None)

### erp_px_cat_g1v2 table
df_px_cat = df_px_cat.where(pd.notnull(df_px_cat), None)

# %% [markdown]
# ## Staging erp datafiles

# %% [markdown]
# ### Staging erp_loc_a101_stg table 

# %%
cursor = conn.cursor()
insert_sql = """
INSERT INTO erp_loc_a101_stg(
    cid,
    cntry
)
VALUES (%s, %s)
"""
for _, row in df_loc_a101.iterrows():
    cursor.execute(insert_sql, tuple(row))
conn.commit()

print("Insert completed")

# %% [markdown]
# ### Staging erp_cust_az12_stg table 

# %%
cursor = conn.cursor()
insert_sql = """
INSERT INTO erp_cust_az12_stg(
    cid,
    bdate,
    gen
)
VALUES (%s, %s, %s)
"""
for _, row in df_cust_az12.iterrows():
    cursor.execute(insert_sql, tuple(row))
conn.commit()

print("Insert completed")

# %% [markdown]
# ### Staging erp_px_cat_g1v2_stg table 

# %%
cursor = conn.cursor()
insert_sql = """
INSERT INTO erp_px_cat_g1v2_stg(
    id,
    cat,
    subcat,
    maintenance
)
VALUES (%s, %s, %s, %s)
"""
for _, row in df_px_cat.iterrows():
    cursor.execute(insert_sql, tuple(row))
conn.commit()

print("Insert completed")


