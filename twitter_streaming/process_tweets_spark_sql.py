from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession \
    .builder \
    .appName("TopHashatgs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "sparkdesk.ic.metz.supelec.fr") \
    .option("port", 9009) \
    .load()

hastags = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("hashtag")
).where(col("hashtag").startswith("#"))

hashtagCounts = hastags.groupBy("hashtag").count().orderBy('count', ascending = False)

query = hashtagCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
