# Databricks notebook source
# MAGIC %md
# MAGIC ### **DLT Pipeline**

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming table**

# COMMAND ----------

# Expectations
my_rules = {
     "rule1" : "product_id IS NOT NULL",
     "rule2" : "product_name IS NOT NULL"
    

}

# COMMAND ----------

@dp.table
@dp.expect_all_or_drop(my_rules)
def DimProducts_stage():
    return spark.readStream.table(
        "databricks_cata.silver.products_silver"
    )

# COMMAND ----------

@dp.temporary_view
def DimProducts_view():
    return spark.readStream.table("DimProducts_stage")

# COMMAND ----------

dp.create_streaming_table("DimProducts")

# COMMAND ----------

dp.create_auto_cdc_flow(
    target="DimProducts",
    source="DimProducts_view",
    keys=["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)

# COMMAND ----------

