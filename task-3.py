import findspark
findspark.init()
from pyspark.sql import SparkSession
import threading
spark=SparkSession.builder\
	.appName("Concurrency Demo")\
	.getOrCreate()
sc=spark.sparkContext
numbers=list(range(1,11))
rdd=sc.parallelize(numbers,4)
def process_partition(partition):
	thread_name=threading.current_thread().name
	result=[]
	for num in partition:
		result.append(f"Thread:{thread_name},Num:{num},Square:{num*num}")
	return iter(result)
output=rdd.mapPartitions(process_partition)
print("Concurrent Processing Results:")
for item in output.collect():
	print(item)
spark.stop()
