import time
from pyspark.sql import SparkSession


def map_to_part_of_days(row):
    time_occurred = row["TIME OCC"]
    part_of_day = "a"
    if 500 <= time_occurred < 1200:
        part_of_day = "morning"
    elif 1200 <= time_occurred < 1700:
        part_of_day = "afternoon"
    elif 1700 <= time_occurred < 2100:
        part_of_day = "evening"
    else:
        part_of_day = "night"

    return (part_of_day, 1)


spark = (
    SparkSession.builder.config("spark.executor.instances", 4)
    .appName("Query 2 RDD execution")
    .getOrCreate()
)
start_time = time.time()

data_rdd = spark.read.parquet(
    "hdfs://okeanos-master:54310/user/user/input/base_data"
).rdd

street_crimes = data_rdd.filter(lambda x: x["Premis Desc"] == "STREET")
mapped_to_parts_crimes = street_crimes.map(map_to_part_of_days)
result = mapped_to_parts_crimes.reduceByKey(lambda x, y: x + y).sortBy(
    lambda x: x[1], ascending=False
)

result.coalesce(1).saveAsTextFile(
    "hdfs://okeanos-master:54310/user/user/output/q2_rdd_res"
)
print(f"Execution Time: {time.time()-start_time} seconds")
