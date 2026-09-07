import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder \
	.appName("MapManipulationRDD")\
		.getOrCreate()
sc = spark.sparkContext
data=[(101, "Ravi"),(102, "Priya"),(103,"Kiran")]
rdd=sc.parallelize(data)
print("original key-Value Pairs:")
print(rdd.collect())
new_rdd=rdd.union(sc.parallelize([(104,"Anita")]))
print("After adding anita")
print(new_rdd.collect())
updated_rdd=new_rdd.map(lambda x: (x[0],"pooja") if x[0]==102 else x)
print("After updating key 102")
print(updated_rdd.collect())
value=updated_rdd.lookup(101)
print("Value for key 101:")
print(value)
exists=updated_rdd.lookup(103)
if exists:
	print("\n Key 103 exists in the RDD")
deleted_rdd = updated_rdd.filter(lambda x: x[0] !=103)
print("After deleting key 103:")
print(deleted_rdd.collect())
print("Keys:")
print(deleted_rdd.keys().collect())
print("Values:")
print(deleted_rdd.values().collect())
print("Final key-value pairs")
print(deleted_rdd.collect())
spark.stop()