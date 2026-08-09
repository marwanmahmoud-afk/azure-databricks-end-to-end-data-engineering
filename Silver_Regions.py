# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@databricksete3.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@databricksete3.dfs.core.windows.net/regions")
df.display()

# COMMAND ----------

df = spark.read.format("delta")\
        .load("abfss://silver@databricksete3.dfs.core.windows.net/customers")
    
df.display()    

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@databricksete3.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksete3.dfs.core.windows.net/regions'