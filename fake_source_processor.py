from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("FakeNumberStreamer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1000) \
    .option("rampUpTime", 5) \
    .load()

numberCount = lines \
    .withColumn("value", round(log10("value"), 0)) \
    .groupBy("value") \
    .count() \
    .sort(col("count").desc())

# numberCount.sql("select value, count from value order by count desc limit 10")

query = numberCount \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
