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
ssc.checkpoint("checkpoint")

# The Kafka broker hosts and topic used to write to Kafka
kafkaBrokers="wn0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092,wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092"
kafkaTopic="ratings"

print("Finished setting Kafka broker and topic configuration.")



# Create a StreamingContext with batch interval of 5 seconds


kafkaStream = KafkaUtils.createDirectStream(ssc, ['ratings'], {"metadata.broker.list":kafkaBrokers})

def updateFunc(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)
    # add the new values with the previous running count to get the new count

running_counts = kafkaStream.map(lambda tuple: tuple[1]).map(lambda x: x.split(",")).map(lambda x:x[1])\
                      .map(lambda word: (word, 1))\
                      .reduceByKey(lambda a, b: a + b) \
                      .updateStateByKey(updateFunc)

counts_sorted = running_counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))

def printResults(rdd):
    print("Total 5 Movie ids: ", rdd.count())
    print(rdd.take(5))

counts_sorted.foreachRDD(printResults)

# kafkaStream.pprint()

ssc.start()  # Start the computation
print("Start")
ssc.awaitTermination(25)  # Wait for the computation to terminate
ssc.stop(stopSparkContext=True)  # Stop the StreamingContext without stopping the SparkContext

print("Finished")


