# Databricks notebook source
# MAGIC %md
# MAGIC CUSTOMERS DATASET

# COMMAND ----------

bronze_customers_df = spark.table("workspace.bronze.customers")

# COMMAND ----------

bronze_customers_df.count()

# COMMAND ----------

display(bronze_customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Check NULL values

# COMMAND ----------

from pyspark.sql import functions as F

bronze_customers_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in bronze_customers_df.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicate customers

# COMMAND ----------

silver_customers_df = (
    bronze_customers_df
    .dropDuplicates(["customer_id"])
)

# COMMAND ----------

silver_customers_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Trim unnecessary spaces

# COMMAND ----------

from pyspark.sql import functions as F

silver_customers_df = (
    silver_customers_df
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email", F.trim(F.col("email")))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
)

# COMMAND ----------

display(
    silver_customers_df.limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize Email and Phone

# COMMAND ----------

silver_customers_df = (
    silver_customers_df
    .withColumn(
        "email",
        F.lower(F.col("email"))
    )
    .withColumn(
        "phone",
        F.regexp_replace(F.col("phone"), "[^0-9]", "")
    )
)

# COMMAND ----------

display(
    silver_customers_df.select(
        "customer_id",
        "customer_name",
        "email",
        "phone"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Customer IDs and Emails

# COMMAND ----------

silver_customers_df = (
    silver_customers_df
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("email").isNotNull())
)

# COMMAND ----------

print("Silver customer count:", silver_customers_df.count())

# COMMAND ----------

silver_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.customers")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.silver.customers
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTS DATASET

# COMMAND ----------

bronze_products_df = spark.table("workspace.bronze.products")

# COMMAND ----------

bronze_products_df.count()

# COMMAND ----------

display(bronze_products_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicate Products

# COMMAND ----------

silver_products_df = (
    bronze_products_df
    .dropDuplicates(["product_id"])
)

# COMMAND ----------

silver_products_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Clean Product Text Columns

# COMMAND ----------

from pyspark.sql import functions as F

silver_products_df = (
    silver_products_df
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("brand", F.trim(F.col("brand")))
)

# COMMAND ----------

display(
    silver_products_df.limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Product Price

# COMMAND ----------

silver_products_df.select(
    F.min("price").alias("minimum_price"),
    F.max("price").alias("maximum_price"),
    F.avg("price").alias("average_price"),
    F.sum(F.col("price").isNull().cast("int")).alias("null_prices")
).show()

# COMMAND ----------

silver_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.products")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.silver.products
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC STORES DATASET

# COMMAND ----------

bronze_stores_df = spark.table("workspace.bronze.stores")

# COMMAND ----------

bronze_stores_df.count()

# COMMAND ----------

display(bronze_stores_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicate Stores

# COMMAND ----------

silver_stores_df = (
    bronze_stores_df
    .dropDuplicates(["store_id"])
)

# COMMAND ----------

silver_stores_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

silver_stores_df = (
    silver_stores_df
    .withColumn("store_name", F.trim(F.col("store_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("manager_name", F.trim(F.col("manager_name")))
)

# COMMAND ----------

display(
    silver_stores_df.limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Store Data

# COMMAND ----------

silver_stores_df.select(
    F.sum(F.col("store_id").isNull().cast("int")).alias("null_store_id"),
    F.sum(F.col("store_name").isNull().cast("int")).alias("null_store_name"),
    F.sum(F.col("city").isNull().cast("int")).alias("null_city"),
    F.sum(F.col("manager_name").isNull().cast("int")).alias("null_manager")
).show()

# COMMAND ----------

silver_stores_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.stores")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.silver.stores
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC SALES DATASET

# COMMAND ----------

bronze_sales_df = spark.table("workspace.bronze.sales")

# COMMAND ----------

bronze_sales_df.count()

# COMMAND ----------

display(bronze_sales_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicate Transactions

# COMMAND ----------

silver_sales_df = (
    bronze_sales_df
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

silver_sales_df.count()

# COMMAND ----------

silver_sales_df = (
    bronze_sales_df
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

silver_sales_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Check for NULL values

# COMMAND ----------

silver_sales_df.select(
    F.sum(F.col("transaction_id").isNull().cast("int")).alias("null_transaction_id"),
    F.sum(F.col("transaction_date").isNull().cast("int")).alias("null_transaction_date"),
    F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer_id"),
    F.sum(F.col("product_id").isNull().cast("int")).alias("null_product_id"),
    F.sum(F.col("store_id").isNull().cast("int")).alias("null_store_id"),
    F.sum(F.col("quantity").isNull().cast("int")).alias("null_quantity"),
    F.sum(F.col("unit_price").isNull().cast("int")).alias("null_unit_price"),
    F.sum(F.col("payment_method").isNull().cast("int")).alias("null_payment_method")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check quantity and price

# COMMAND ----------

silver_sales_df.select(
    F.min("quantity").alias("min_quantity"),
    F.max("quantity").alias("max_quantity"),
    F.min("unit_price").alias("min_unit_price"),
    F.max("unit_price").alias("max_unit_price")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create total_amount in Silver Sales

# COMMAND ----------

silver_sales_df = (
    silver_sales_df
    .withColumn(
        "total_amount",
        F.round(
            F.col("quantity") * F.col("unit_price"),
            2
        )
    )
)

# COMMAND ----------

display(
    silver_sales_df.select(
        "transaction_id",
        "quantity",
        "unit_price",
        "total_amount"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the calculated amount

# COMMAND ----------

silver_sales_df.select(
    F.min("total_amount").alias("min_total_amount"),
    F.max("total_amount").alias("max_total_amount"),
    F.avg("total_amount").alias("avg_total_amount"),
    F.sum(F.col("total_amount").isNull().cast("int")).alias("null_total_amount")
).show()

# COMMAND ----------

display(
    silver_sales_df.select(
        "transaction_id",
        "quantity",
        "unit_price",
        "total_amount"
    ).limit(10)
)

# COMMAND ----------

silver_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.sales")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.silver.sales
""").show()

# COMMAND ----------

display(
    spark.table("workspace.silver.sales").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC