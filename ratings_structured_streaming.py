import os
import pyspark
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
	
	
# The Kafka broker hosts and topic used to write to Kafka
kafkaBrokers="wn0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092,wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092"
kafkaTopic="ratings"

print("Finished setting Kafka broker and topic configuration.")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaBrokers) \
  .option("subscribe", kafkaTopic) \
  .option("startingOffsets", "earliest") \
  .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
query = query.writeStream.format("console").start()
query.awaitTermination(30)
query.stop()