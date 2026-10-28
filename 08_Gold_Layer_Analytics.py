# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_customers_df = spark.table("workspace.silver.customers")
silver_products_df = spark.table("workspace.silver.products")
silver_stores_df = spark.table("workspace.silver.stores")
silver_sales_df = spark.table("workspace.silver.sales")

# COMMAND ----------

silver_sales_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Customer Sales Summary

# COMMAND ----------

customer_sales_df = (
    silver_sales_df
    .join(
        silver_customers_df,
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

customer_sales_summary_df = (
    customer_sales_df
    .groupBy(
        "customer_id",
        "customer_name",
        "city",
        "state"
    )
    .agg(
        F.countDistinct("transaction_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("total_amount"), 2).alias("average_order_value")
    )
)

# COMMAND ----------

display(
    customer_sales_summary_df
    .orderBy(F.desc("total_sales"))
    .limit(10)
)

# COMMAND ----------

customer_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.customer_sales_summary"
    )

# COMMAND ----------

spark.table(
    "workspace.gold.customer_sales_summary"
).count()

# COMMAND ----------

display(
    spark.table(
        "workspace.gold.customer_sales_summary"
    )
    .orderBy(F.desc("total_sales"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Product Sales Summary

# COMMAND ----------

product_sales_df = (
    silver_sales_df
    .join(
        silver_products_df,
        on="product_id",
        how="left"
    )
)

# COMMAND ----------

product_sales_summary_df = (
    product_sales_df
    .groupBy(
        "product_id",
        "product_name",
        "category",
        "brand"
    )
    .agg(
        F.countDistinct("transaction_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("unit_price"), 2).alias("average_unit_price")
    )
)

# COMMAND ----------

product_sales_df = (
    silver_sales_df
    .join(
        silver_products_df,
        silver_sales_df.product_id == 
        F.concat(
            F.lit("P"),
            F.lpad(
                silver_products_df.product_id.cast("string"),
                4,
                "0"
            )
        ),
        how="left"
    )
)

# COMMAND ----------

display(product_sales_df.limit(10))

# COMMAND ----------

product_sales_summary_df = (
    product_sales_df
    .groupBy(
        "product_id",
        "product_name",
        "category",
        "brand"
    )
    .agg(
        F.countDistinct("transaction_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("total_amount"), 2).alias("average_order_value")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

product_sales_df = (
    silver_sales_df.alias("s")
    .join(
        silver_products_df.alias("p"),
        F.col("s.product_id") ==
        F.concat(
            F.lit("P"),
            F.lpad(
                F.col("p.product_id").cast("string"),
                4,
                "0"
            )
        ),
        "left"
    )
    .select(
        F.col("s.product_id").alias("product_id"),
        F.col("s.transaction_id"),
        F.col("s.quantity"),
        F.col("s.unit_price"),
        F.col("s.total_amount"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("p.price").alias("product_list_price")
    )
)

display(product_sales_df.limit(10))

# COMMAND ----------

product_sales_summary_df = (
    product_sales_df
    .groupBy(
        "product_id",
        "product_name",
        "category",
        "brand"
    )
    .agg(
        F.countDistinct("transaction_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("total_amount"), 2).alias("average_order_value")
    )
)

display(product_sales_summary_df.limit(10))

# COMMAND ----------

product_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.product_sales_summary")

# COMMAND ----------

spark.table("workspace.gold.product_sales_summary").count()

# COMMAND ----------

display(
    spark.table("workspace.gold.product_sales_summary")
    .orderBy(F.desc("total_sales"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Store Sales Summary

# COMMAND ----------

store_sales_df = (
    silver_sales_df.alias("s")
    .join(
        silver_stores_df.alias("st"),
        F.col("s.store_id") == F.col("st.store_id"),
        "left"
    )
    .select(
        F.col("s.store_id").alias("store_id"),
        F.col("s.transaction_id"),
        F.col("s.quantity"),
        F.col("s.total_amount"),
        F.col("st.store_name"),
        F.col("st.city"),
        F.col("st.manager_name")
    )
)

display(store_sales_df.limit(10))

# COMMAND ----------

store_sales_summary_df = (
    store_sales_df
    .groupBy(
        "store_id",
        "store_name",
        "city",
        "manager_name"
    )
    .agg(
        F.countDistinct("transaction_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("total_amount"), 2).alias("average_order_value")
    )
)

display(store_sales_summary_df.limit(10))

# COMMAND ----------

store_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.store_sales_summary")

# COMMAND ----------

spark.table("workspace.gold.store_sales_summary").count()

# COMMAND ----------

display(
    spark.table("workspace.gold.store_sales_summary")
    .orderBy(F.desc("total_sales"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Monthly Sales Summary

# COMMAND ----------

monthly_sales_summary_df = (
    silver_sales_df
    .withColumn(
        "sales_month",
        F.date_format("transaction_date", "yyyy-MM")
    )
    .groupBy("sales_month")
    .agg(
        F.countDistinct("transaction_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("total_amount"), 2).alias("average_order_value")
    )
    .orderBy("sales_month")
)

display(monthly_sales_summary_df)

# COMMAND ----------

monthly_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.monthly_sales_summary")

# COMMAND ----------

spark.table("workspace.gold.monthly_sales_summary").count()

# COMMAND ----------

display(
    spark.table("workspace.gold.monthly_sales_summary")
    .orderBy("sales_month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the Complete Medallion Architecture

# COMMAND ----------

spark.sql("""
SHOW TABLES IN workspace.bronze
""").show()

# COMMAND ----------

print("Bronze Customers:", spark.table("workspace.bronze.customers").count())
print("Bronze Products :", spark.table("workspace.bronze.products").count())
print("Bronze Stores   :", spark.table("workspace.bronze.stores").count())
print("Bronze Sales    :", spark.table("workspace.bronze.sales").count())

# COMMAND ----------

spark.sql("""
SHOW TABLES IN workspace.silver
""").show()

# COMMAND ----------

spark.sql("""
SHOW TABLES IN workspace.gold
""").show()