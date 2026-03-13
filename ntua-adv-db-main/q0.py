from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date


spark = SparkSession.builder.appName("Query 0 execution").getOrCreate()

df = spark.read.csv(
    "hdfs://okeanos-master:54310/user/user/input/Crime_Data_from_2010_to_2019.csv",
    header=True,
    inferSchema=True,
)
df = df.union(
    spark.read.csv(
        "hdfs://okeanos-master:54310/user/user/input/Crime_Data_from_2020_to_Present.csv",
        header=True,
        inferSchema=True,
    )
).dropDuplicates()

date_columns = ["Date Rptd", "DATE OCC"]
for col_name in date_columns:
    df = df.withColumn(col_name, to_date(col_name, "MM/dd/yyyy hh:mm:ss a"))
df.printSchema()
print(f"Number of rows in the DataFrame: {df.count()}")

df.write.mode("overwrite").parquet(
    "hdfs://okeanos-master:54310/user/user/input/base_data"
)
