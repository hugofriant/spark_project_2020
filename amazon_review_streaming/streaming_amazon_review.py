from pyspark.sql import *
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

PATH = "hdfs://sar01:9000/spark_project_2020/review_stream/"

schema = StructType() \
    .add('marketplace', 'string') \
    .add('customer_id', 'integer') \
    .add('review_id', 'string') \
    .add('product_id', 'string') \
    .add('product_parent', 'integer') \
    .add('product_title', 'string') \
    .add('product_catgory', 'string') \
    .add('star_rating', 'integer') \
    .add('helpful_votes', 'integer') \
    .add('total_votes', 'integer') \
    .add('vine', 'string') \
    .add('verified_purchase', 'string') \
    .add('review_headline', 'string') \
    .add('review_body', 'string') \
    .add('review_date', 'timestamp')

spark = SparkSession \
    .builder \
    .master("spark://sar01:7077") \
    .appName("AmazonReviewStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir(
    'hdfs://sar01:9000/spark_project_2020/checkpoint')

df = spark.readStream \
    .csv(path=PATH, schema=schema, sep='\t', header=True)

products = df \
    .select("product_id") \
    .distinct()
products = products.withColumn("type", lit("product"))

customers = df \
    .select("customer_id") \
    .distinct()
customers = customers.withColumn("type", lit("customer"))

vertices = products.union(customers).withColumnRenamed('product_id', 'id')
edges = df.select('customer_id', 'product_id')
edges = edges.withColumnRenamed('customer_id', 'src').withColumnRenamed(
    'product_id', 'dst')
edges = edges.withColumn("relation", lit("bought"))

query = vertices \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path",
            "hdfs://sar01:9000/spark_project_2020/streaming_output/vertices.csv") \
    .option("checkpointLocation",
            "hdfs://sar01:9000/spark_project_2020/checkpoint/vertices") \
    .start()

query_2 = edges \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path",
            "hdfs://sar01:9000/spark_project_2020/streaming_output/edges.csv") \
    .option("checkpointLocation",
            "hdfs://sar01:9000/spark_project_2020/checkpoint/edges") \
    .start()

query.awaitTermination()
