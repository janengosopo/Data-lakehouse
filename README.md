# Data Lakehouse on Databricks


I built a data lakehouse in Databricks that takes raw CSV files from two source systems (CRM and ERP), cleans them, and turns them into tables that are ready for sales reporting.

**Tools used:** Databricks, PySpark, Spark SQL, Delta Lake

---

## The Big Picture

The project follows the medallion architecture.
![Architecture diagram](Data_lakehouse.jpg)

---

## Source Data

6 CSV files from 2 systems:

| System | File | What it holds |
|--------|------|---------------|
| CRM | `cust_info.csv` | Customer details |
| CRM | `prd_info.csv` | Product details |
| CRM | `sales_details.csv` | Sales orders |
| ERP | `CUST_AZ12.csv` | Customer birth date and gender |
| ERP | `LOC_A101.csv` | Customer country |
| ERP | `PX_CAT_G1V2.csv` | Product categories |

---

## What Each Layer Does

### Bronze: load the raw data
- One notebook (`bronze_layer.py`) loads all 6 files.
- The files are listed in a simple config list, so adding a new file means adding one entry, not writing new code.
- Each file is saved as a Delta table.

### Silver: clean the data
One notebook per table. Common steps in all of them: remove extra spaces from text columns and rename columns to clear names (for example `cst_gndr` → `gender`).

### Gold: build the reporting tables
The Gold layer is a **star schema**: one fact table in the middle, with dimension tables around it.

| Table | Type | Description |
|-------|------|-------------|
| `dim_customers` | Dimension | One row per customer. Joins CRM customers with ERP birth date, gender and country. If gender is missing in the CRM, it uses the ERP value. |
| `dim_products` | Dimension | One row per product. Joins CRM products with ERP categories. |
| `fact_sales` | Fact | One row per sales order line, linked to the customer and product tables, with dates, amount, quantity and price. |

Both dimension tables have a generated **surrogate key** (`customer_key`, `product_key`). The fact table uses these keys to link back to them.

`gold_orchestration.py` runs the three Gold notebooks in the right order (dimensions first, then the fact table). It can be used as a single entry point for a Databricks Job.

---
