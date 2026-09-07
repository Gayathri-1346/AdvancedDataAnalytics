
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create Spark Session
spark = SparkSession.builder \
    .appName("SparkStreamingWordCount") \
    .getOrCreate()

# Create Streaming DataFrame from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

# Count occurrences of words
word_counts = words.groupBy("word").count()

# Display output on console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Keep streaming active
query.awaitTermination()

