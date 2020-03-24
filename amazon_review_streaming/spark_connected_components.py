import time

from graphframes import *
from pyspark.sql import *
from pyspark.sql.functions import lit


PATH = 'hdfs://sar01:9000/spark_project_2020/amazon_review_dataset/'

spark = SparkSession.builder \
    .master("spark://sar01:7077") \
    .appName("ConnectedComponnents") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir(
    'hdfs://sar01:9000/spark_project_2020/checkpoint')

start = time.time()

df = spark.read.load(PATH, format="csv", sep="\t", inferSchema="true",
                     header="true")

products = df \
    .select("product_id") \
    .distinct()
products = products.withColumn("type", lit("product"))

customers = df.select("customer_id") \
    .distinct()
customers = customers.withColumn("type", lit("customer"))

vertices = products.union(customers).withColumnRenamed('product_id', 'id')
edges = df.select('customer_id', 'product_id')
edges = edges.withColumnRenamed('customer_id', 'src').withColumnRenamed(
    'product_id', 'dst')
edges = edges.withColumn("relation", lit("bought"))

g = GraphFrame(vertices, edges)

result = g.connectedComponents()
result.select("id", "component").orderBy("component").show()

print(time.time() - start)
