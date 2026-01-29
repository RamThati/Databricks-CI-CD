# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
data = [
    (1, "Ram"),
    (2, "Databricks"),
    (3, "Jenkins")
]

df = spark.createDataFrame(data, ["id", "name"])

# COMMAND ----------
df.show()

# COMMAND ----------
df.write.mode("overwrite").saveAsTable("default.sample_table")