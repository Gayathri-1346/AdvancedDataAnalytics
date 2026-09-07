# Import findspark
import findspark
findspark.init()

# Import Spark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Create Spark Session
spark = SparkSession.builder \
    .appName("StudentOperations") \
    .getOrCreate()

# -------------------------------------------------
# Create Student Data
# -------------------------------------------------

data = [
    (301, "Nikhil", "AIML", 89, 20),
    (302, "Pooja", "CSE", 95, 21),
    (303, "Rahul", "ECE", None, 20),
    (304, "Sneha", "IT", 81, 22),
    (305, "Tarun", "MECH", 76, 21),
    (306, "Uma", "EEE", 88, None),
    (307, None, "CSE", 91, 20),
    (308, "Vikram", "AIML", 84, 22),
    (309, "Yamini", "IT", 93, 21),
    (310, "Zahid", "ECE", 79, 20),

    # Duplicate Records
    (302, "Pooja", "CSE", 95, 21),
    (309, "Yamini", "IT", 93, 21)
]

columns = ["StudentID", "Name", "Department", "Marks", "Age"]

student_df = spark.createDataFrame(data, columns)

print("Original Student Data")
student_df.show()

# =====================================================
# a. Count NULL values throughout all columns
# =====================================================

print("NULL Values in Each Column")

student_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in student_df.columns
]).show()

# =====================================================
# b. Remove Duplicate Records
# =====================================================

print("Data After Removing Duplicate Records")

student_df = student_df.dropDuplicates()

student_df.show()

# =====================================================
# c. Get Top 3 Students Based on Marks
# =====================================================

print("Top 3 Students Based on Marks")

student_df.orderBy(col("Marks").desc()).show(3)

# =====================================================
# d. Group By Department
# =====================================================

print("Department-wise Student Count")

student_df.groupBy("Department") \
          .count() \
          .show()

print("Department-wise Average Marks")

student_df.groupBy("Department") \
          .avg("Marks") \
          .show()

# Stop Spark Session
spark.stop()
