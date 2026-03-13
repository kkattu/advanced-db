import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, udf, avg, count
from pyspark.sql.types import DoubleType
from math import radians, sin, cos, sqrt, atan2


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


spark = SparkSession.builder.appName("Query 4 (Area) execution").getOrCreate()
start_time = time.time()

base_df = (
    spark.read.parquet("hdfs://okeanos-master:54310/user/user/input/base_data")
    .select("DATE OCC", "LAT", "LON", "Weapon Used Cd", "AREA ")
    .filter(
        (col("LAT") != 0.0) & (col("LON") != 0.0) & (col("Weapon Used Cd").like("1__"))
    )
)  # Remove records with no lat long and weapon used is not a firearm.

lapd_df = (
    spark.read.csv(
        "hdfs://okeanos-master:54310/user/user/input/LAPD_Police_Stations.csv",
        header=True,
        inferSchema=True,
    )
    .select("DIVISION", "PREC", "X", "Y")
    .withColumnRenamed("PREC", "AREA ")
)

joined_on_area_df = base_df.join(
    lapd_df.hint("BROADCAST"), on="AREA ", how="inner"
).withColumn(
    "distance", get_distance("Y", "X", "LAT", "LON")
)  # join on area and calculate distance between the location of the crime and the location of the pd station
joined_on_area_df.explain(mode="formatted")
grouped_by_year_df = (
    joined_on_area_df.withColumn("year", year("DATE OCC"))
    .groupBy("year")
    .agg(avg("distance").alias("avg_distance"), count("*").alias("count"))
    .orderBy("year")
)
grouped_by_year_df.write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q4_area_grouped_by_year"
)

grouped_by_division_df = (
    joined_on_area_df.groupBy("DIVISION")
    .agg(avg("distance").alias("avg_distance"), count("*").alias("count"))
    .orderBy("count", ascending=False)
)

grouped_by_division_df.write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q4_area_grouped_by_division"
)

print(f"Execution Time: {time.time()-start_time} seconds")
