import pyspark
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


sc = pyspark.SparkContext()
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)

# The Kafka broker hosts and topic used to write to Kafka
kafkaBrokers="wn0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092,wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092"
kafkaTopic="ratings"

print("Finished setting Kafka broker and topic configuration.")



# Create a StreamingContext with batch interval of 5 seconds


kafkaStream = KafkaUtils.createDirectStream(ssc, ['ratings'], {"metadata.broker.list":kafkaBrokers})
kafkaStream.pprint()

ssc.start()  # Start the computation
print("Start")
ssc.awaitTermination(50)  # Wait for the computation to terminate
ssc.stop(stopSparkContext=True)  # Stop the StreamingContext without stopping the SparkContext

print("Finished")


