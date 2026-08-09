# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

dbutils.widgets.text("init_load_flag", "1")

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

print("init_load_flag =", init_load_flag)

# COMMAND ----------

df = spark.sql("""
    SELECT *
    FROM databricks_cata.silver.customers_silver
""")

df.display()

# COMMAND ----------

df = spark.sql("""
    SELECT *
    FROM databricks_cata.silver.customers_silver
""")

df.display()

# COMMAND ----------

if init_load_flag == 0:

    df_old = spark.sql("""
        SELECT
            DimCustomerKey,
            customer_id,
            email,
            city,
            state,
            domains,
            full_name,
            update_date
        FROM databricks_cata.gold.DimCustomers
    """)

else:

    df_old = spark.createDataFrame(
        [],
        """
        DimCustomerKey BIGINT,
        customer_id STRING,
        email STRING,
        city STRING,
        state STRING,
        domains STRING,
        full_name STRING,
        update_date TIMESTAMP
        """
    )

df_old.printSchema()

# COMMAND ----------

df_old = df_old \
    .withColumnRenamed("DimCustomerKey", "old_DimCustomerKey") \
    .withColumnRenamed("customer_id", "old_customer_id") \
    .withColumnRenamed("email", "old_email") \
    .withColumnRenamed("city", "old_city") \
    .withColumnRenamed("state", "old_state") \
    .withColumnRenamed("domains", "old_domains") \
    .withColumnRenamed("full_name", "old_full_name") \
    .withColumnRenamed("update_date", "old_update_date")

df_old.printSchema()

# COMMAND ----------

df_join = df.join(
    df_old,
    df["customer_id"] == df_old["old_customer_id"],
    "left"
)

df_join.display()

# COMMAND ----------

df_new = df_join.filter(
    col("old_DimCustomerKey").isNull()
)

df_new.display()

# COMMAND ----------

df_old = df_join.filter(
    col("old_DimCustomerKey").isNotNull()
).select(
    col("customer_id"),
    col("email"),
    col("city"),
    col("state"),
    col("domains"),
    col("full_name"),
    col("old_DimCustomerKey").alias("DimCustomerKey"),
    current_timestamp().alias("update_date")
)

df_old.printSchema()
df_old.display()

# COMMAND ----------

df_new = df_new.select(
    "customer_id",
    "email",
    "city",
    "state",
    "domains",
    "full_name"
)

df_new = df_new.withColumn(
    "update_date",
    current_timestamp()
)

df_new.printSchema()
df_new.display()

# COMMAND ----------

df_new = df_new.withColumn(
    "DimCustomerKey",
    monotonically_increasing_id() + lit(1)
)

df_new.printSchema()
df_new.display()

# COMMAND ----------

if init_load_flag == 1:

    max_surrogate_key = 0

else:

    df_maxsur = spark.sql("""
        SELECT MAX(DimCustomerKey) AS max_surrogate_key
        FROM databricks_cata.gold.DimCustomers
    """)

    max_surrogate_key = df_maxsur.collect()[0]["max_surrogate_key"]

    if max_surrogate_key is None:
        max_surrogate_key = 0

print("Max surrogate key =", max_surrogate_key)

# COMMAND ----------

df_new = df_new.withColumn(
    "DimCustomerKey",
    col("DimCustomerKey") + lit(max_surrogate_key)
)

df_new.printSchema()
df_new.display()

# COMMAND ----------

final_columns = [
    "customer_id",
    "email",
    "city",
    "state",
    "domains",
    "full_name",
    "DimCustomerKey",
    "update_date"
]

df_new = df_new.select(final_columns)

df_old = df_old.select(final_columns)

print("===== DF_NEW =====")
df_new.printSchema()

print("===== DF_OLD =====")
df_old.printSchema()

# COMMAND ----------

df_final = df_new.unionByName(df_old)

df_final.printSchema()
df_final.display()

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomers"):
    print("Gold table exists")
else:
    print("Gold table does not exist")

# COMMAND ----------

if init_load_flag == 1:

    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option(
            "path",
            "abfss://gold@databricksete3.dfs.core.windows.net/DimCustomers"
        ) \
        .saveAsTable(
            "databricks_cata.gold.DimCustomers"
        )

    print("Initial Load Completed")

else:

    dlt_obj = DeltaTable.forPath(
        spark,
        "abfss://gold@databricksete3.dfs.core.windows.net/DimCustomers"
    )

    dlt_obj.alias("trg") \
        .merge(
            df_final.alias("src"),
            "trg.customer_id = src.customer_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print("Incremental Load Completed")

# COMMAND ----------

df_gold = spark.sql("""
    SELECT *
    FROM databricks_cata.gold.DimCustomers
    ORDER BY DimCustomerKey
""")

df_gold.printSchema()
df_gold.display()