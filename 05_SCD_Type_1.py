# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_products_df = spark.table("workspace.silver.products")

# COMMAND ----------

display(
    silver_products_df
    .filter(F.col("product_id") == 1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the SCD Type 1 table

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.products_scd1
AS
SELECT *
FROM workspace.silver.products
""")

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.silver.products_scd1
WHERE product_id = 1
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a changed product record

# COMMAND ----------

changed_product_df = (
    silver_products_df
    .filter(F.col("product_id") == 1)
    .withColumn("price", F.lit(2500.00))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the old and new values

# COMMAND ----------

display(
    changed_product_df.select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "price"
    )
)

# COMMAND ----------

changed_product_df.createOrReplaceTempView("changed_product")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform the MERGE

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.silver.products_scd1 AS target
USING changed_product AS source
ON target.product_id = source.product_id

WHEN MATCHED THEN
  UPDATE SET
    target.product_name = source.product_name,
    target.category = source.category,
    target.brand = source.brand,
    target.price = source.price
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the change

# COMMAND ----------

spark.sql("""
SELECT
    product_id,
    product_name,
    category,
    brand,
    price
FROM workspace.silver.products_scd1
WHERE product_id = 1
""").show()

# COMMAND ----------

spark.sql("""
SELECT
    product_id,
    price
FROM workspace.silver.products_scd1
WHERE product_id = 1
""").show()

# COMMAND ----------

spark.sql("""
SELECT
    product_id,
    price
FROM workspace.silver.products_scd1
WHERE product_id = 1
""").show()

# COMMAND ----------

new_product_df = (
    silver_products_df
    .filter(F.col("product_id") == 2)
)

# COMMAND ----------

new_product_df.createOrReplaceTempView("new_product")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Truly New Product

# COMMAND ----------

new_product_df = spark.createDataFrame(
    [
        (1001, "Product_1001", "Electronics", "Apple", 3500.00)
    ],
    [
        "product_id",
        "product_name",
        "category",
        "brand",
        "price"
    ]
)

# COMMAND ----------

display(new_product_df)

# COMMAND ----------

new_product_df.createOrReplaceTempView("new_product")

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.silver.products_scd1 AS target
USING new_product AS source
ON target.product_id = source.product_id

WHEN MATCHED THEN
  UPDATE SET
    target.product_name = source.product_name,
    target.category = source.category,
    target.brand = source.brand,
    target.price = source.price

WHEN NOT MATCHED THEN
  INSERT (
    product_id,
    product_name,
    category,
    brand,
    price
  )
  VALUES (
    source.product_id,
    source.product_name,
    source.category,
    source.brand,
    source.price
)
""")

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.silver.products_scd1
WHERE product_id = 1001
""").show()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS total_products
FROM workspace.silver.products_scd1
""").show()

# COMMAND ----------

spark.sql("""
SELECT product_id, price
FROM workspace.silver.products_scd1
WHERE product_id IN (1, 1001)
ORDER BY product_id
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC