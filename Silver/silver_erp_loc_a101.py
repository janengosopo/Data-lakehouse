# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze table

# COMMAND ----------

df = spark.table("workspace.bronze.erp_loc_a101")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore data

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

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

df = df.withColumn("cid", F.regexp_replace(col("cid"), "-", ""))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize country name

# COMMAND ----------

df = df.withColumn(
    "cntry",
    F.when(col("cntry") == "DE", "Germany")
     .when(col("cntry").isin("US", "USA"), "United States")
     .when((col("cntry") == "") | col("cntry").isNull(), "n/a")
     .otherwise(col("cntry"))
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming Columns

# COMMAND ----------

RENAME_MAP = {
    "cid": "customer_number",
    "cntry": "country"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

### Validate dataframe

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Write tlo silver table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_customer_location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.erp_customer_location LIMIT 10