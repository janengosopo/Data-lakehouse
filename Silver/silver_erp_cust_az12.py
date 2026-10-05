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

df = spark.table("workspace.bronze.erp_cust_az12")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore data

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trim all columns

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup customer id

# COMMAND ----------

df = df.withColumn(
    "cid",
    F.when(col("cid").startswith("NAS"),
           F.substring(col("cid"), 4, F.length(col("cid"))))
     .otherwise(col("cid"))
)


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate birthdate

# COMMAND ----------

df = df.withColumn(
    "bdate",
    F.when(col("bdate") > F.current_date(), None)
     .otherwise(col("bdate"))
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize gender column

# COMMAND ----------

df = df.withColumn(
    "gen",
    F.when(F.upper(col("gen")).isin("F", "FEMALE"), "Female")
     .when(F.upper(col("gen")).isin("M", "MALE"), "Male")
     .otherwise("n/a")
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

RENAME_MAP = {
    "cid": "customer_number",
    "bdate": "birth_date",
    "gen": "gender"
}
for old_name, new_name in RENAME_MAP.items():
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

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.erp_customers LIMIT 10