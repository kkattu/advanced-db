from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, desc, year, regexp_replace, count, when
import time

spark = SparkSession.builder.appName("DF query 3 execution").getOrCreate()
start_time = time.time()

df = spark.read.parquet("hdfs://okeanos-master:54310/user/user/input/base_data")

revgeocoding_df = spark.read.csv(
    ["hdfs://okeanos-master:54310/user/user/input/revgecoding.csv"], header=True
)
income_df = spark.read.csv(
    ["hdfs://okeanos-master:54310/user/user/input/LA_income_2015.csv"], header=True
)

income_df = income_df.filter(col("Community").like("Los Angeles%"))
income_df = income_df.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[^0-9.]", "").cast(DoubleType()),
)

df = df.withColumn("Year", year(col("DATE OCC")))
df = df.filter(col("Year") == 2015)

df = df.filter(col("Vict Descent").isNotNull())
df = df.select("Vict Descent", "LAT", "LON")

# df = df.join(revgeocoding_df, on=["LAT", "LON"], how="inner")
# df = df.join(revgeocoding_df.hint("BROADCAST"), on=["LAT", "LON"], how="inner")
# df = df.join(revgeocoding_df.hint("MERGE"), on=["LAT", "LON"], how="inner")
df = df.join(revgeocoding_df.hint("SHUFFLE_HASH"), on=["LAT", "LON"], how="inner")
# df = df.join(revgeocoding_df.hint("SHUFFLE_REPLICATE_NL"), on=["LAT", "LON"], how="inner")

df = df.withColumnRenamed("ZIPcode", "Zip Code")

# income_df = income_df.join(df.select("Zip Code"), on="Zip Code", how="inner")
# income_df = income_df.join(df.select("Zip Code").hint("BROADCAST"), on="Zip Code", how="inner")
# income_df = income_df.join(df.select("Zip Code").hint("MERGE"), on="Zip Code", how="inner")
income_df = income_df.join(
    df.select("Zip Code").hint("SHUFFLE_HASH"), on="Zip Code", how="inner"
)
# income_df = income_df.join(df.select("Zip Code").hint("SHUFFLE_REPLICATE_NL"), on="Zip Code", how="inner")

income_df = income_df.distinct()
high_income = income_df.orderBy(col("Estimated Median Income").desc()).limit(3)
low_income = income_df.orderBy(col("Estimated Median Income").asc()).limit(3)

income = high_income.union(low_income)

# res = df.join(income.select("Zip Code"), on="Zip Code", how="inner").select("Vict Descent")
# res = df.join(income.select("Zip Code").hint("BROADCAST"), on="Zip Code", how="inner").select("Vict Descent")
# res = df.join(income.select("Zip Code").hint("MERGE"), on="Zip Code", how="inner").select("Vict Descent")
res = df.join(
    income.select("Zip Code").hint("SHUFFLE_HASH"), on="Zip Code", how="inner"
).select("Vict Descent")
# res = df.join(income.select("Zip Code").hint("SHUFFLE_REPLICATE_NL"), on="Zip Code", how="inner").select("Vict Descent")

descent = res.groupBy("Vict Descent").agg(count("*").alias("#"))
descent = descent.orderBy(desc("#"))

descent_mapping = {
    "A": "Other Asian",
    "B": "Black",
    "C": "Chinese",
    "D": "Cambodian",
    "F": "Filipino",
    "G": "Guamanian",
    "H": "Hispanic/Latin/Mexican",
    "I": "American Indian/Alaskan Native",
    "J": "Japanese",
    "K": "Korean",
    "L": "Laotian",
    "O": "Other",
    "P": "Pacific Islander",
    "S": "Samoan",
    "U": "Hawaiian",
    "V": "Vietnamese",
    "W": "White",
    "X": "Unknown",
    "Z": "Asian Indian",
}
for code, descr in descent_mapping.items():
    descent = descent.withColumn(
        "Vict Descent",
        when(col("Vict Descent") == code, descr).otherwise(col("Vict Descent")),
    )

descent.select("Vict Descent", "#").alias("Victim Descent")

descent.write.mode("overwrite").option("header", True).csv(
    "hdfs://okeanos-master:54310/user/user/output/q3"
)

df.explain(mode="formatted")
income_df.explain(mode="formatted")
res.explain(mode="formatted")

print(f"Execution Time: {time.time()-start_time} seconds")
