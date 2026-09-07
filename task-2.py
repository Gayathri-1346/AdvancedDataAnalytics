import findspark
findspark.init()
from pyspark.sql import SparkSession
spark= SparkSession.builder\
       .appName("ListManipulationRDD")\
       .getOrCreate()
sc=spark.sparkContext

# print("List operations")
# data=["mango","apple","custard apple","sapota", "banana","mango"]
# rdd=sc.parallelize(data)
# print("Original data")
# print(rdd.collect())

# # append
# new_rdd=rdd.union(sc.parallelize(["mango"]))
# print("After adding mango")
# print(new_rdd.collect())

# # update
# updated_rdd=new_rdd.map(lambda x: "grapes" if x=="apple" else x)
# print("Updated RDD")
# print(updated_rdd.collect())

# #remove
# deleted_rdd=updated_rdd.filter(lambda x: x!="banana")
# print("Deleted RDD")
# print(deleted_rdd.collect())

# #sort
# sorted_rdd=deleted_rdd.sortBy(lambda x:x)
# print("After sorting")
# print(sorted_rdd.collect())

# #Count
# print("Frequency:",sorted_rdd.count())

# #distinct ele
# print("Distinct elements:", deleted_rdd.distinct().collect())

# #to get first ele
# print("Distinct elements:", deleted_rdd.first())

# result=(rdd.map(lambda x: (x,1))
#         .reduceByKey(lambda a, b: a+b))
# print(result.collect())

# print("Tuple Operations using PySpark")

# # Tuple data
# data = (102, 2, 20, 30, 40)

# # Create RDD
# rdd = sc.parallelize(data)

# # Original Tuple
# print("Original Tuple:")
# print(rdd.collect())

# # Append
# append_rdd = rdd.union(sc.parallelize([50]))

# print("\nAfter Append:")
# print(append_rdd.collect())

# # Lookup (get element at index 2)
# print("\nLookup Element:")
# print(rdd.collect()[2])

# # Slice (get elements from index 1 to 3)
# # Slice (get elements from index 1 to 3)
# slice_rdd = sc.parallelize(append_rdd.collect()[1:4])

# print("\nSlice:")
# print(slice_rdd.collect())
# slice_tuple=sc.parallelize(slice_rdd)


print("Set operations")
print("Set Operations using PySpark")

# Set data
set1 = {10, 20, 30, 40}
set2 = {30, 40, 50, 60}

# Create RDDs
rdd1 = sc.parallelize(list(set1))
rdd2 = sc.parallelize(list(set2))

# Original Sets
print("Set 1:")
print(rdd1.collect())

print("\nSet 2:")
print(rdd2.collect())

# Union
union_rdd = rdd1.union(rdd2).distinct()

print("\nUnion:")
print(union_rdd.collect())

# Insertion 
insert_rdd = rdd1.union(sc.parallelize([70])).distinct()

print("\nAfter Insertion:")
print(insert_rdd.collect())

# Difference 
difference_rdd = rdd1.subtract(rdd2)

print("\nDifference:")
print(difference_rdd.collect())

# Symmetric Difference 
sym_diff_rdd = rdd1.subtract(rdd2).union(rdd2.subtract(rdd1))

print("\nSymmetric Difference:")
print(sym_diff_rdd.collect())

spark.stop()