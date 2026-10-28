# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_customers_df = spark.table(
    "workspace.silver.customers"
)

# COMMAND ----------

silver_customers_df.count()

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.customers_scd3 AS
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    city AS current_city,
    CAST(NULL AS STRING) AS previous_city,
    state AS current_state,
    CAST(NULL AS STRING) AS previous_state,
    registration_date
FROM workspace.silver.customers
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd3")
    .limit(10)
)

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd3")
    .filter(F.col("customer_id") == "C00001")
)

# COMMAND ----------

changed_customer_df = (
    silver_customers_df
    .filter(F.col("customer_id") == "C00001")
    .withColumn("city", F.lit("Chennai"))
    .withColumn("state", F.lit("Tamil Nadu"))
)

# COMMAND ----------

display(changed_customer_df)

# COMMAND ----------

spark.sql("""
UPDATE workspace.silver.customers_scd3
SET
    previous_city = current_city,
    current_city = 'Chennai',
    previous_state = current_state,
    current_state = 'Tamil Nadu'
WHERE customer_id = 'C00001'
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd3")
    .filter(F.col("customer_id") == "C00001")
)

# COMMAND ----------

spark.sql("""
SELECT
    customer_id,
    current_city,
    previous_city,
    current_state,
    previous_state
FROM workspace.silver.customers_scd3
WHERE customer_id = 'C00001'
""").show(truncate=False)

# COMMAND ----------

spark.table("workspace.silver.customers_scd3").count()

# COMMAND ----------

changed_customer_df = (
    silver_customers_df
    .filter(F.col("customer_id") == "C00001")
    .withColumn("city", F.lit("Mumbai"))
    .withColumn("state", F.lit("Maharashtra"))
)

# COMMAND ----------

display(changed_customer_df)

# COMMAND ----------

spark.sql("""
UPDATE workspace.silver.customers_scd3
SET
    previous_city = current_city,
    current_city = 'Mumbai',
    previous_state = current_state,
    current_state = 'Maharashtra'
WHERE customer_id = 'C00001'
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd3")
    .filter(F.col("customer_id") == "C00001")
)

# COMMAND ----------

spark.sql("""
SELECT
    customer_id,
    current_city,
    previous_city,
    current_state,
    previous_state
FROM workspace.silver.customers_scd3
WHERE customer_id = 'C00001'
""").show(truncate=False)

# COMMAND ----------

spark.table("workspace.silver.customers_scd3").count()