# Data Lakehouse in Databricks


I built a data lakehouse in Databricks that takes raw CSV files from two source systems (CRM and ERP), cleans them, and turns them into tables that are ready for sales reporting.

**Tools used:** Databricks, PySpark, Spark SQL, Delta Lake

---

## Project overview

The project follows the medallion architecture.
![Architecture diagram](Data%20lakehouse.jpg)

---

## Source Data
The data comes from a public data set. They are imported to Databricks to simulate data coming from ERP and CRM.

| System | File | What it has |
|--------|------|---------------|
| CRM | `cust_info.csv` | Customer details |
| CRM | `prd_info.csv` | Product details |
| CRM | `sales_details.csv` | Sales orders |
| ERP | `CUST_AZ12.csv` | Customer birth date and gender |
| ERP | `LOC_A101.csv` | Customer country |
| ERP | `PX_CAT_G1V2.csv` | Product categories and sub-categories|

---

## What Each Layer Does

### Bronze: load the raw data
- One notebook (`bronze_layer.py`) loads all 6 files.
- The files are listed in a simple config list, so adding a new file means adding one entry, not writing new code.
- Each file is saved as a Delta table.

### Silver: clean the data
One notebook per table. Each table requires different transformation steps.
Common steps in all of them: remove extra spaces from text columns and rename columns to clear names (for example `cst_gndr` → `gender`).

### Gold: build the reporting tables
The Gold layer is a **star schema**: one fact table in the middle, with dimension tables around it.

| Table | Type | Description |
|-------|------|-------------|
| `dim_customers` | Dimension | One row per customer. Joins CRM customers with ERP birth date, gender and country. If gender is missing in the CRM, it uses the ERP value. |
| `dim_products` | Dimension | One row per product. Joins CRM products with ERP categories. |
| `fact_sales` | Fact | One row per sales order line, linked to the customer and product tables, with dates, amount, quantity and price. |

## Pipeline orchestration
The orchestration is done using Jobs in Databricks.
In the gold layer, `gold_orchestration.py` runs the three Gold notebooks in the right order (dimensions first, then the fact table).

---
