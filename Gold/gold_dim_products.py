# Databricks notebook source
# MAGIC %md
# MAGIC #Transformation Logic

# COMMAND ----------

query = """

SELECT

    ROW_NUMBER() OVER (
        ORDER BY prod.start_date, prod.product_number
    ) AS product_key, -- Surrogate key

    prod.product_id,
    prod.product_number,
    prod.product_name,
    prod.category_id,
    pc.category,
    pc.sub_category,
    pc.maintenance_flag,
    prod.product_line,
    prod.start_date

FROM workspace.silver.crm_products AS prod

LEFT JOIN workspace.silver.erp_product_category AS pc
    ON prod.category_id = pc.category_id

WHERE prod.end_date IS NULL 

"""

df = spark.sql(query)

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Gold Table

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.gold.dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dim_products LIMIT 10