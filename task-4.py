import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import threading
spark=SparkSession.builder\
	.appName("Concurrency Demo")\
	.getOrCreate()
sc=spark.sparkContext
data=[
	(101,"Ravi",50000),
(102,"Priya",60000),
(103,"Kiran",45000),
(104,"Anitha",70000)]
df=spark.createDataFrame(data,["EmpID","Name","Salary"])
print("Original DataFrame")
df.show()
print("Selected Columns:")
df.select("Name","Salary").show()
print("Employees with salary > 50000:")
df.filter(col("Salary")>50000).show()
print("DataFarme with bonus column:")
df.withColumn("Bonus",col("Salary")*0.10).show()
print("Sorted by salary")
df.orderBy(col("Salary").desc()).show()
new_row=[(105,"anil",65000)]
new_df=spark.createDataFrame(new_row,["EmpID","Name","Salary"])
df=df.unionByName(new_df)
print("Updated DataFrame")
df.show()
spark.stop()
