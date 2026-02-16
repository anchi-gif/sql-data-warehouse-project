# sql-data-warehouse-project

Building a modern *Data Warehouse solution using MySQL*, including ETL processes, dimensional modeling, and analytical reporting.

---

## 📖 Project Overview

This project demonstrates the design and implementation of a modern *Data Warehouse architecture* using MySQL.

It covers the full pipeline from raw data ingestion to business-ready analytical views.

The project is inspired by the Data with Baraa SQL course and has been adapted to MySQL, including syntax adjustments and feature substitutions where required.

---

## 🏗️ Architecture Overview

The project follows a layered Data Warehouse architecture:

1. *Bronze Layer* – Raw data ingestion  
   - Data loaded from CSV files using Python (Pandas)  
   - Stored without transformation  

2. *Silver Layer* – Data cleansing & standardization  
   - Data validation  
   - Null handling  
   - Deduplication  
   - Data type normalization  
   - Business rule enforcement  

3. *Gold Layer* – Business-ready dimensional model  
   - Fact table for transactional sales  
   - Dimension tables for customers and products  
   - Optimized for analytical queries  

An architecture diagram was designed using Draw.io to document data flow and system structure.

---

## 🔄 ETL Process

The ETL workflow follows these structured steps:

- *Extract*  
  Raw data loaded from CSV files using Python (Pandas)

- *Transform*  
  Data cleaning and transformation performed inside MySQL using:
  - Stored procedures
  - Window functions
  - Conditional logic
  - Data validation rules

- *Load*  
  Cleaned data inserted into structured warehouse tables (Bronze → Silver → Gold)

---

## 🧱 Data Modeling

The warehouse is designed using *Dimensional Modeling principles*:

- Star schema design
- Surrogate keys (ROW_NUMBER)
- Fact table for measurable business metrics
- Dimension tables for descriptive attributes
- Data integrity validation

---

## 🛠️ Tools & Technologies

- *MySQL 8.x* – Relational Database
- *MySQL Workbench* – Database management
- *Python (Pandas)* – Data ingestion
- *CSV Files* – Source data
- *Git & GitHub* – Version control
- *Draw.io* – Architecture & data modeling diagrams
---
## 📄 License
This project is licenced under the MIT License. See the `LICENSE` file for details.
