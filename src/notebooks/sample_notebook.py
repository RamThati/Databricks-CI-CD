from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
 
data = [
    (1, "Ram"),
    (2, "Databricks"),
    (3, "Jenkins")
]
 
df = spark.createDataFrame(data, ["id", "name"])
 
df.show()
 
df.write.mode("overwrite").saveAsTable("default.sample_table")