from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Training Job") \
    .getOrCreate()


print("Start training job")
print("Training finished")


spark.stop()