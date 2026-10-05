# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze table

# COMMAND ----------

df = spark.table("workspace.bronze.crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore data

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trim data

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize data

# COMMAND ----------


df = (
    df
    .withColumn(
        "cst_marital_status",
        F.when(F.upper(F.col("cst_marital_status")) == "S", "Single")
         .when(F.upper(F.col("cst_marital_status")) == "M", "Married")
         .otherwise("n/a")
    )
    .withColumn(
        "cst_gndr",
        F.when(F.upper(F.col("cst_gndr")) == "F", "Female")
         .when(F.upper(F.col("cst_gndr")) == "M", "Male")
         .otherwise("n/a")
    )
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove records not having customer id

# COMMAND ----------

df = df.filter(col("cst_id").isNotNull())

# COMMAND ----------

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

column_name_map = {
    "cst_id": "customer_id",
    "cst_key": "customer_number",
    "cst_firstname": "first_name",
    "cst_lastname": "last_name",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "created_date"
}
for old_name, new_name in column_name_map.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate dataframe

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to silver table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.crm_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.crm_customers LIMIT 10