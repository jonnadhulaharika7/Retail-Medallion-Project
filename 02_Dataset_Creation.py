# Databricks notebook source
# MAGIC %md
# MAGIC CUSTOMERS DATASET

# COMMAND ----------

from pyspark.sql import functions as F

customers_df = (
    spark.range(1, 10001)
    .withColumnRenamed("id", "customer_number")
    .withColumn(
        "customer_id",
        F.concat(F.lit("C"), F.lpad(F.col("customer_number"), 5, "0"))
    )
    .withColumn(
        "customer_name",
        F.concat(
            F.lit("Customer_"),
            F.col("customer_number")
        )
    )
    .withColumn(
        "email",
        F.concat(
            F.lit("customer"),
            F.col("customer_number"),
            F.lit("@retailcorp.com")
        )
    )
    .withColumn(
        "phone",
        F.concat(
            F.lit("9"),
            F.lpad(F.col("customer_number"), 9, "0")
        )
    )
    .withColumn(
        "city",
        F.expr("""
            element_at(
                array(
                    'Nellore',
                    'Hyderabad',
                    'Chennai',
                    'Bangalore',
                    'Vijayawada',
                    'Mumbai',
                    'Pune',
                    'Delhi',
                    'Kolkata',
                    'Ahmedabad'
                ),
                cast(rand() * 10 as int) + 1
            )
        """)
    )
    .withColumn(
        "state",
        F.when(F.col("city") == "Nellore", "Andhra Pradesh")
         .when(F.col("city") == "Hyderabad", "Telangana")
         .when(F.col("city") == "Chennai", "Tamil Nadu")
         .when(F.col("city") == "Bangalore", "Karnataka")
         .when(F.col("city") == "Vijayawada", "Andhra Pradesh")
         .when(F.col("city") == "Mumbai", "Maharashtra")
         .when(F.col("city") == "Pune", "Maharashtra")
         .when(F.col("city") == "Delhi", "Delhi")
         .when(F.col("city") == "Kolkata", "West Bengal")
         .when(F.col("city") == "Ahmedabad", "Gujarat")
    )
    .withColumn(
        "registration_date",
        F.expr("""
            date_add(
                to_date('2024-01-01'),
                cast(rand() * 900 as int)
            )
        """)
    )
    .drop("customer_number")
)

# COMMAND ----------

customers_df.count()

# COMMAND ----------

display(customers_df.limit(10))

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Save customers as a Bronze Delta table

# COMMAND ----------

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.customers")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.bronze.customers
""").show()

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM workspace.bronze.customers
        LIMIT 10
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTS DATASET

# COMMAND ----------

from pyspark.sql import functions as F

products_df = (
    spark.range(1, 1001)
    .withColumnRenamed("id", "product_id")
    .withColumn(
        "product_name",
        F.concat(F.lit("Product_"), F.col("product_id"))
    )
)

# COMMAND ----------

products_df.count()

# COMMAND ----------

spark.range(1, 1001)

# COMMAND ----------

display(products_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

products_df = (
    products_df
    .withColumn(
        "category",
        F.expr("""
            element_at(
                array(
                    'Electronics',
                    'Clothing',
                    'Home Appliances',
                    'Groceries',
                    'Furniture',
                    'Sports',
                    'Beauty',
                    'Books'
                ),
                cast(rand() * 8 as int) + 1
            )
        """)
    )
    .withColumn(
        "brand",
        F.expr("""
            element_at(
                array(
                    'Samsung',
                    'Nike',
                    'LG',
                    'Apple',
                    'Puma',
                    'Sony',
                    'Adidas',
                    'Dell'
                ),
                cast(rand() * 8 as int) + 1
            )
        """)
    )
    .withColumn(
        "price",
        F.round(F.rand() * 4900 + 100, 2)
    )
)

# COMMAND ----------

products_df.count()

# COMMAND ----------

display(products_df.limit(10))

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Save products as a Bronze Delta table

# COMMAND ----------

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.products")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.bronze.products
""").show()

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM workspace.bronze.products
        LIMIT 10
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC STORES DATASET

# COMMAND ----------

from pyspark.sql import functions as F

stores_df = (
    spark.range(1, 101)
    .withColumnRenamed("id", "store_number")
    .withColumn(
        "store_id",
        F.concat(
            F.lit("S"),
            F.lpad(F.col("store_number"), 3, "0")
        )
    )
    .withColumn(
        "store_name",
        F.concat(
            F.lit("RetailCorp_Store_"),
            F.col("store_number")
        )
    )
    .withColumn(
        "city",
        F.expr("""
            element_at(
                array(
                    'Nellore',
                    'Hyderabad',
                    'Chennai',
                    'Bangalore',
                    'Vijayawada',
                    'Mumbai',
                    'Pune',
                    'Delhi',
                    'Kolkata',
                    'Ahmedabad'
                ),
                cast(rand() * 10 as int) + 1
            )
        """)
    )
    .withColumn(
        "manager_name",
        F.concat(
            F.lit("Manager_"),
            F.col("store_number")
        )
    )
    .drop("store_number")
)

# COMMAND ----------

stores_df.count()

# COMMAND ----------

display(stores_df.limit(10))

# COMMAND ----------

stores_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Save Stores as a Bronze Delta table

# COMMAND ----------

stores_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.stores")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.bronze.stores
""").show()

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM workspace.bronze.stores
        LIMIT 10
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC SALES DATASET

# COMMAND ----------

from pyspark.sql import functions as F

sales_df = (
    spark.range(1, 100001)
    .withColumnRenamed("id", "transaction_number")

    .withColumn(
        "transaction_id",
        F.concat(
            F.lit("T"),
            F.lpad(F.col("transaction_number"), 6, "0")
        )
    )

    .withColumn(
        "transaction_date",
        F.expr("""
            date_add(
                to_date('2025-01-01'),
                cast(rand() * 365 as int)
            )
        """)
    )

    .withColumn(
        "customer_id",
        F.concat(
            F.lit("C"),
            F.lpad(
                (F.floor(F.rand() * 10000) + 1).cast("int"),
                5,
                "0"
            )
        )
    )

    .withColumn(
        "product_id",
        F.concat(
            F.lit("P"),
            F.lpad(
                (F.floor(F.rand() * 1000) + 1).cast("int"),
                4,
                "0"
            )
        )
    )

    .withColumn(
        "store_id",
        F.concat(
            F.lit("S"),
            F.lpad(
                (F.floor(F.rand() * 100) + 1).cast("int"),
                3,
                "0"
            )
        )
    )

    .withColumn(
        "quantity",
        (F.floor(F.rand() * 5) + 1).cast("int")
    )

    .withColumn(
        "unit_price",
        F.round(F.rand() * 4900 + 100, 2)
    )

    .withColumn(
        "payment_method",
        F.expr("""
            element_at(
                array(
                    'UPI',
                    'Credit Card',
                    'Debit Card',
                    'Cash',
                    'Net Banking'
                ),
                cast(rand() * 5 as int) + 1
            )
        """)
    )

    .drop("transaction_number")
)

# COMMAND ----------

sales_df.count()

# COMMAND ----------

display(sales_df.limit(10))

# COMMAND ----------

sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Save Sales as a Bronze Delta table

# COMMAND ----------

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.sales")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.bronze.sales
""").show()

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM workspace.bronze.sales
        LIMIT 10
    """)
)

# COMMAND ----------

print("Customers:", customers_df.count())
print("Products :", products_df.count())
print("Stores   :", stores_df.count())
print("Sales    :", sales_df.count())

# COMMAND ----------

print("Customers columns:", customers_df.columns)
print("Products columns :", products_df.columns)
print("Stores columns   :", stores_df.columns)
print("Sales columns    :", sales_df.columns)