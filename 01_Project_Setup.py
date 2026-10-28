# Databricks notebook source
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS").show()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.gold
""")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.gold
""")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()