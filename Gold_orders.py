# Databricks notebook source
# MAGIC %md
# MAGIC ### **FACT ORDERS**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.orders_silver")
df.display()

# COMMAND ----------

df_dimcus = spark.sql("""
SELECT
    DimCustomerKey,
    customer_id AS dim_customer_id
FROM databricks_cata.gold.dimcustomers
""")

df_dimpro = spark.sql("""
SELECT
    product_id AS DimProductKey,
    product_id AS dim_product_id
FROM databricks_cata.gold.dimproducts
""")

# COMMAND ----------

spark.sql("SHOW TABLES IN databricks_cata.gold").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fact DataFrame**

# COMMAND ----------

df_fact = (
    df
    .join(
        df_dimcus,
        df["customer_id"] == df_dimcus["dim_customer_id"],
        how="left"
    )
    .join(
        df_dimpro,
        df["product_id"] == df_dimpro["dim_product_id"],
        how="left"
    )
)

df_fact_new = df_fact.drop(
    "dim_customer_id",
    "dim_product_id",
    "customer_id",
    "product_id"
)

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on Fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

df_fact_new = df_fact_new.dropDuplicates([
    "order_id",
    "DimCustomerKey",
    "DimProductKey"
])

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("databricks_cata.gold.FactOrders"):

    dlt_obj = DeltaTable.forName(
        spark,
        "databricks_cata.gold.FactOrders"
    )

    (
        dlt_obj.alias("trg")
        .merge(
            df_fact_new.alias("src"),
            """
            trg.order_id = src.order_id
            AND trg.DimCustomerKey = src.DimCustomerKey
            AND trg.DimProductKey = src.DimProductKey
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        df_fact_new.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("databricks_cata.gold.FactOrders")
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.factorders

# COMMAND ----------

