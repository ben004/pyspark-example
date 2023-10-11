from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Replace 'YourAppName' with a name for your Spark application
app_name = "YourAppName"

# Create a SparkSession with the specified configuration
spark = SparkSession.builder.appName(app_name).master("spark://127.0.0.1:7077").getOrCreate()

try:
  rdd = spark.sparkContext.parallelize(range(1, 100))
  print("THE SUM IS HERE: ", rdd.sum())
finally:
  spark.stop()
