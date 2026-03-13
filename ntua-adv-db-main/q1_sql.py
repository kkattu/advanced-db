from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder.config("spark.executor.instances", 4)
    .appName("Query_1_sql execution")
    .getOrCreate()
)
start_time = time.time()

df = spark.read.parquet("hdfs://okeanos-master:54310/user/user/input/base_data")

df.createOrReplaceTempView("my_table")

result_df = spark.sql(
    """
    SELECT year, month, crime_total, row_num
    FROM (SELECT year, month, crime_total, row_number() OVER (PARTITION BY year ORDER BY crime_total DESC) as row_num
    FROM (
        SELECT year(`DATE OCC`) as year, month(`DATE OCC`) as month, count(*) as crime_total
        FROM my_table
        GROUP BY year, month
    ) )
    WHERE row_num <= 3
    ORDER BY year, row_num
"""
)

result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q1_sql_res"
)
print(f"Execution Time: {time.time()-start_time} seconds")
