from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, year, month, count, row_number
import time

spark = (
    SparkSession.builder.config("spark.executor.instances", 4)
    .appName("Query_1_df execution")
    .getOrCreate()
)
start_time = time.time()

df = spark.read.parquet("hdfs://okeanos-master:54310/user/user/input/base_data")

window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())
grouped_df = (
    df.withColumn("year", year("DATE OCC"))
    .withColumn("month", month("DATE OCC"))
    .groupBy("year", "month")
    .agg(count("*").alias("crime_total"))
    .withColumn("row_num", row_number().over(window_spec))
    .filter("row_num <= 3")
    .orderBy("year", "row_num")
)

grouped_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q1_df_res"
)
print(f"Execution Time: {time.time()-start_time} seconds")
