from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    udf,
    avg,
    count,
    row_number,
    monotonically_increasing_id,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import time


@udf(returnType=DoubleType())
def get_distance(lat1, lon1, lat2, lon2):
    R = 6371.4

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance


spark = SparkSession.builder.appName("Query_4_2 execution").getOrCreate()
start_time = time.time()

base_df = (
    spark.read.parquet("hdfs://okeanos-master:54310/user/user/input/base_data")
    .select("DATE OCC", "LAT", "LON", "Weapon Used Cd")
    .filter(
        (col("LAT") != 0.0) & (col("LON") != 0.0) & (col("Weapon Used Cd").like("1__"))
    )
    .withColumn("column_id", monotonically_increasing_id())
)  # Remove records with no lat long and weapon used is not a firearm. Add primary key.

lapd_df = spark.read.csv(
    "hdfs://okeanos-master:54310/user/user/input/LAPD_Police_Stations.csv",
    header=True,
    inferSchema=True,
).select("DIVISION", "X", "Y")

window_spec = Window.partitionBy("column_id").orderBy("min_distance")

cross_joined_df = (
    base_df.crossJoin(lapd_df.hint("BROADCAST"))
    .withColumn("min_distance", get_distance("Y", "X", "LAT", "LON"))
    .withColumn("row_num", row_number().over(window_spec))
    .filter("row_num = 1")
)
cross_joined_df.explain(mode="formatted")
closest_station_by_year_df = (
    cross_joined_df.withColumn("year", year("DATE OCC"))
    .groupBy("year")
    .agg(avg("min_distance").alias("avg_distance"), count("*").alias("count"))
    .orderBy("year")
)

closest_station_by_year_df.write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q4_closest_grouped_by_year"
)

closest_station_by_division_df = (
    cross_joined_df.groupBy("DIVISION")
    .agg(avg("min_distance").alias("avg_distance"), count("*").alias("count"))
    .orderBy("count", ascending=False)
)

closest_station_by_division_df.write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q4_closest_grouped_by_division"
)
print(f"Execution Time: {time.time()-start_time} seconds")
