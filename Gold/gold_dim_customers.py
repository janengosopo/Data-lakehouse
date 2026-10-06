# Databricks notebook source
# MAGIC %md
# MAGIC #Transformation Logic

# COMMAND ----------

query = """

SELECT

    ROW_NUMBER() OVER (ORDER BY crm.customer_id) AS customer_key,
    crm.customer_id,
    crm.customer_number,
    crm.first_name,
    crm.last_name,
    loc.country,
    crm.marital_status,

    CASE
        WHEN crm.gender <> 'n/a' THEN crm.gender
        ELSE COALESCE(erp.gender, 'n/a')
    END AS gender,

    erp.birth_date AS birthdate,
    crm.created_date AS create_date

FROM workspace.silver.crm_customers AS crm

LEFT JOIN workspace.silver.erp_customers AS erp
    ON crm.customer_number = erp.customer_number

LEFT JOIN workspace.silver.erp_customer_location AS loc
    ON crm.customer_number = loc.customer_number

"""

df = spark.sql(query)

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dim_customers LIMIT 10