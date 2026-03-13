import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

spark = (
    SparkSession.builder.config("spark.executor.instances", 4)
    .appName("Query 2 DF execution")
    .getOrCreate()
)
start_time = time.time()

df = spark.read.parquet("hdfs://okeanos-master:54310/user/user/input/base_data").select(
    "Premis Desc", "TIME OCC"
)


streetcrimes_df = df.filter(col("Premis Desc") == "STREET")
streetcrimes_df = streetcrimes_df.withColumn(
    "part of day",
    when((col("TIME OCC") >= 500) & (col("TIME OCC") < 1200), "Morning")
    .when((col("TIME OCC") >= 1200) & (col("TIME OCC") < 1700), "Afternoon")
    .when((col("TIME OCC") >= 1700) & (col("TIME OCC") < 2100), "Evening")
    .otherwise("Night"),
)

streetcrimes_df = streetcrimes_df.groupBy("part of day").agg(
    count("*").alias("crime_count")
)
streetcrimes_df = streetcrimes_df.orderBy(col("crime_count").desc())

streetcrimes_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q2_df_res"
)
print(f"Execution Time: {time.time()-start_time} seconds")
