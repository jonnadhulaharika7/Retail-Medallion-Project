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
CREATE TABLE IF NOT EXISTS workspace.silver.customers_scd2 AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
    customer_id,
    customer_name,
    email,
    phone,
    city,
    state,
    registration_date,
    registration_date AS effective_start_date,
    CAST('9999-12-31' AS DATE) AS effective_end_date,
    TRUE AS current_flag
FROM workspace.silver.customers
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd2").limit(10)
)

# COMMAND ----------

spark.table("workspace.silver.customers_scd2").count()

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd2")
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

changed_customer_df.createOrReplaceTempView("changed_customer")

# COMMAND ----------

spark.sql("""
SELECT *
FROM changed_customer
""").show()

# COMMAND ----------

changed_customer_df.createOrReplaceTempView("changed_customer")

# COMMAND ----------

spark.sql("""
SELECT *
FROM changed_customer
""").show()

# COMMAND ----------

spark.sql("""
UPDATE workspace.silver.customers_scd2
SET
    effective_end_date = DATE '2026-09-09',
    current_flag = FALSE
WHERE customer_id = 'C00001'
  AND current_flag = TRUE
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd2")
    .filter(F.col("customer_id") == "C00001")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Insert the New Chennai Version

# COMMAND ----------

spark.sql("""
INSERT INTO workspace.silver.customers_scd2
(
    customer_sk,
    customer_id,
    customer_name,
    email,
    phone,
    city,
    state,
    registration_date,
    effective_start_date,
    effective_end_date,
    current_flag
)
SELECT
    MAX(tgt.customer_sk) + 1,
    src.customer_id,
    src.customer_name,
    src.email,
    src.phone,
    src.city,
    src.state,
    src.registration_date,
    DATE '2026-09-10',
    DATE '9999-12-31',
    TRUE
FROM changed_customer AS src
CROSS JOIN workspace.silver.customers_scd2 AS tgt
WHERE src.customer_id = 'C00001'
GROUP BY
    src.customer_id,
    src.customer_name,
    src.email,
    src.phone,
    src.city,
    src.state,
    src.registration_date
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd2")
    .filter(F.col("customer_id") == "C00001")
    .orderBy("effective_start_date")
)

# COMMAND ----------

spark.sql("""
SELECT
    customer_sk,
    customer_id,
    customer_name,
    city,
    state,
    effective_start_date,
    effective_end_date,
    current_flag
FROM workspace.silver.customers_scd2
WHERE current_flag = TRUE
  AND customer_id = 'C00001'
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    current_flag,
    COUNT(*) AS record_count
FROM workspace.silver.customers_scd2
GROUP BY current_flag
ORDER BY current_flag
""").show()

# COMMAND ----------

spark.table("workspace.silver.customers_scd2").count()

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

# MAGIC %md
# MAGIC Close the Current Chennai Record

# COMMAND ----------

spark.sql("""
UPDATE workspace.silver.customers_scd2
SET
    effective_end_date = DATE '2026-09-10',
    current_flag = FALSE
WHERE customer_id = 'C00001'
  AND current_flag = TRUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Insert the Mumbai Version

# COMMAND ----------

spark.sql("""
INSERT INTO workspace.silver.customers_scd2
(
    customer_sk,
    customer_id,
    customer_name,
    email,
    phone,
    city,
    state,
    registration_date,
    effective_start_date,
    effective_end_date,
    current_flag
)
SELECT
    MAX(tgt.customer_sk) + 1,
    src.customer_id,
    src.customer_name,
    src.email,
    src.phone,
    src.city,
    src.state,
    src.registration_date,
    DATE '2026-09-11',
    DATE '9999-12-31',
    TRUE
FROM changed_customer AS src
CROSS JOIN workspace.silver.customers_scd2 AS tgt
WHERE src.customer_id = 'C00001'
GROUP BY
    src.customer_id,
    src.customer_name,
    src.email,
    src.phone,
    src.city,
    src.state,
    src.registration_date
""")

# COMMAND ----------

display(
    spark.table("workspace.silver.customers_scd2")
    .filter(F.col("customer_id") == "C00001")
    .select(
        "customer_sk",
        "customer_id",
        "city",
        "state",
        "effective_start_date",
        "effective_end_date",
        "current_flag"
    )
    .orderBy("effective_start_date")
)

# COMMAND ----------

spark.sql("""
SELECT
    customer_sk,
    customer_id,
    city,
    state,
    effective_start_date,
    effective_end_date,
    current_flag
FROM workspace.silver.customers_scd2
WHERE customer_id = 'C00001'
ORDER BY effective_start_date
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    customer_id,
    COUNT(*) AS current_records
FROM workspace.silver.customers_scd2
WHERE current_flag = TRUE
  AND customer_id = 'C00001'
GROUP BY customer_id
""").show()

# COMMAND ----------

spark.table("workspace.silver.customers_scd2").count()

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS current_customer_count
FROM workspace.silver.customers_scd2
WHERE current_flag = TRUE
""").show()

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS historical_record_count
FROM workspace.silver.customers_scd2
WHERE current_flag = FALSE
""").show()