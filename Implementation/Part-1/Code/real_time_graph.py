import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Create a SparkSession and StreamingContext
spark = SparkSession.builder.appName("RealTimeGraph").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 1)

# Create a DStream from Kafka topic
kafkaStream = KafkaUtils.createStream(ssc, ZOOKEEPER_QUORUM, GROUP_ID, {KAFKA_TOPIC: 1})

# Parse the JSON data from Kafka topic and extract required fields
parsedStream = kafkaStream.map(lambda x: json.loads(x[1])).map(lambda x: (x['date'], x['sentiment_score'], x['close']))

# Create a window of 10 seconds
windowedStream = parsedStream.window(10, 1)

# Calculate average sentiment score and stock close price for each window
averageStream = windowedStream.map(lambda x: (x[0], (x[1], 1, x[2]))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2])).mapValues(lambda x: (x[0]/x[1], x[2]))

# Convert DStream to RDD and extract data for plotting
def extract_data(rdd):
    if not rdd.isEmpty():
        data = rdd.collect()
        date, score, close = zip(*data)
        return date, score, close

# Create a function to plot data in real-time
def plot_graph():
    plt.clf()
    plt.plot(date, score)
    plt.plot(date, close)
    plt.xlabel("Date")
    plt.ylabel("Sentiment Score / Stock Close Price")
    plt.legend(["Sentiment Score", "Stock Close Price"])
    plt.draw()
    plt.pause(1)

# Start the streaming context and plot data in real-time
ssc.start()
while True:
    data = extract_data(averageStream)
    if data:
        date, score, close = data
        plot_graph()

# Stop the streaming context
ssc.stop()

