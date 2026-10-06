# Databricks notebook source
# MAGIC %md
# MAGIC #Transformation Logic

# COMMAND ----------

query = """

SELECT

    sales.order_number,
    prod.product_key,
    cust.customer_key,
    sales.order_date,
    sales.ship_date,
    sales.due_date,
    sales.sales_amount,
    sales.quantity,
    sales.price

FROM workspace.silver.crm_sales AS sales

LEFT JOIN workspace.gold.dim_products AS prod
    ON sales.product_number = prod.product_number

LEFT JOIN workspace.gold.dim_customers AS cust
    ON sales.customer_id = cust.customer_id

"""

df = spark.sql(query)


# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.fact_sales LIMIT 10