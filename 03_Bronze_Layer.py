# Databricks notebook source
spark.sql("SHOW TABLES IN workspace.bronze").show()

# COMMAND ----------

spark.sql("""
SELECT
    'customers' AS table_name,
    COUNT(*) AS record_count
FROM workspace.bronze.customers

UNION ALL

SELECT
    'products',
    COUNT(*)
FROM workspace.bronze.products

UNION ALL

SELECT
    'stores',
    COUNT(*)
FROM workspace.bronze.stores

UNION ALL

SELECT
    'sales',
    COUNT(*)
FROM workspace.bronze.sales
""").show()