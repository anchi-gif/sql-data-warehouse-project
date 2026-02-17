# Data Dictionary for Gold Layer

## Overview
### The **Gold Layer** is the Business_level data representation, structured to support analytical and reporting use cases.
### It consists of: 
 - **2 dimension tables**
 - **1 fact table for specific business metric**
---
**1. gold.dim_customers**
  - **Purpose:** Store customer details enriched with demographic and geographic attributes.
  - **Columns**

| Column Name      | Data Type   | Description |
|------------------|-------------|------------|
| customer_key     | INT         | Surrogate key uniquely identifying each customer, generated using ROW_NUMBER(). |
| customer_id      | INT         | Original customer identifier from the source system.  |
| customer_number  | VARCHAR(50) | Alphanumeric identifier representing the customer used for tracking and referencing. |
| first_name       | VARCHAR(50) | Customer first name. |
| last_tname       | VARCHAR(50) | Customer last name or family name. |
| country          | VARCHAR(50) | Customer's country of residence (e.g., 'Australia'). |
| marital_status   |VARCHAR(50)  | Marital status (e.g., 'Married'; 'Single').|
| gender           |VARCHAR(50)  | Standardized gender value (e.g., 'Male', 'Female', 'n/a'). |
| birthdate        | DATE        | Date of birth of the customer, formated as YYY-MM-DD (e.g., 1971-10-06). |
| create_date      | DATE        | Date and time when the customer record was created in the system. | 
---
**2. gold.dim_products**
 - **Purpose:** Stores product master data and classification attributes.
 - **Columns**

| Column Name      | Data Type   | Description |
|------------------|-------------|------------|
| product_key      | INT         | Surrogate key uniquely identifying each product, generated using ROW_NUMBER(). |
| product_id       | INT         | Original product identifier from the source system.  |
| product_number   | VARCHAR(50) | Alphanumeric identifier representing the product, used for categorization or invetory. |
| Product_name     | VARCHAR(50) | Descriptive name of the product, including key details such as type, color, and size.|
| category_id      | VARCHAR(50) | An unique identifier for the product's category, linking its to high_level classification.|
| category         | VARCHAR(50) | Broader classifiacation of the product (e.g., 'Bikes', 'Components) to group related items. |
| subcategory      | VARCHAR(50) | More detailed classification of the product within the category, such as prodcut type. |
| maintenance      | VARCHAR(50) | Indicates whether the product requires maintenance (e.g., 'Yes'; 'NO').|
| cost             | INT         | The cost or base price of the product, measured in monetary units. |
| product_line     | VARCHAR(50) | Specific product line or series to which the product belongs (e.g., 'Road', 'Mountain'). |
| start_date       | DATE        | Date when the product became available for sale. | 
---
**3. gold.fact_sales**
   - **Purpose:** Stores transactional sales data for analytic purposes.
   - **Columns**

| **Column Name**  | **Data Type**| **Description** |
|------------------|-------------|------------|
| order_number     | VARCHAR(50) | An unique alphanumeric identifier for each sales order (e.g., 'SO54496'. |
| product_key      | INT         | Surrogate key linking the order to the product dimension table.  |
| customer_key     | INT         | Surrogate key linking the order to the customer dimension table. |
| order_date       | DATE        | Date the order was placed.|
| shipping_date    | DATE        | Date the order was shipped to the customer.|
| due_date         | DATE        | Date payment was due. |
| sales_amount     | INT         | The total monetary value of the sale for the line item in whole currency units (e.g., 25). |
| quantity         | INT         | Number of units ordered for the line item (e.g., 1).|
| price            | INT         | The price per unit of the product for the line item, in whole currency units (e.g., 25). |
