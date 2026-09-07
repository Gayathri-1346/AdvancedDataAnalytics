# Import findspark
import findspark
findspark.init()

# Import Spark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Create Spark Session
spark = SparkSession.builder \
    .appName("MissingValuesAndJoins") \
    .getOrCreate()

# --------------------------------------------------
# Employee DataFrame (15 Records)
# --------------------------------------------------

employee_data = [
    (101, "Ravi", 50000),
    (102, None, 60000),
    (103, "Kiran", None),
    (104, "Anita", 45000),
    (105, None, None),
    (106, "Suresh", 70000),
    (107, "Meena", 52000),
    (108, None, 48000),
    (109, "Rahul", None),
    (110, "Pooja", 65000),
    (111, "Arun", 55000),
    (112, None, 62000),
    (113, "Deepa", None),
    (114, "Vinay", 58000),
    (115, None, 47000)
]

emp_columns = ["EmpID", "Name", "Salary"]

emp_df = spark.createDataFrame(employee_data, emp_columns)

print("Employee DataFrame")
emp_df.show()

# --------------------------------------------------
# Department DataFrame (12 Records)
# --------------------------------------------------

department_data = [
    (101, "HR"),
    (102, "Finance"),
    (103, "IT"),
    (104, "Marketing"),
    (106, "Sales"),
    (107, "HR"),
    (109, "IT"),
    (110, "Finance"),
    (112, "Admin"),
    (114, "Operations"),
    (116, "Testing"),
    (117, "Support")
]

dept_columns = ["EmpID", "Department"]

dept_df = spark.createDataFrame(department_data, dept_columns)

print("Department DataFrame")
dept_df.show()

# --------------------------------------------------
# Count Missing Values
# --------------------------------------------------

print("Missing Values in Each Column")

emp_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in emp_df.columns
]).show()

# --------------------------------------------------
# Replace Missing Values
# --------------------------------------------------

print("Replacing Missing Values")

emp_replace = emp_df.fillna({
    "Name": "Unknown",
    "Salary": 0
})

emp_replace.show()

# --------------------------------------------------
# Remove Missing Values
# --------------------------------------------------

print("Removing Rows with Missing Values")

emp_remove = emp_df.dropna()

emp_remove.show()

# --------------------------------------------------
# INNER JOIN
# --------------------------------------------------

print("Inner Join")

inner_df = emp_replace.join(dept_df, on="EmpID", how="inner")

inner_df.show()

# --------------------------------------------------
# LEFT SEMI JOIN
# --------------------------------------------------

print("Left Semi Join")

semi_df = emp_replace.join(dept_df, on="EmpID", how="left_semi")

semi_df.show()

# --------------------------------------------------
# FULL OUTER JOIN
# --------------------------------------------------

print("Full Outer Join")

outer_df = emp_replace.join(dept_df, on="EmpID", how="outer")

outer_df.show()

# Stop Spark
spark.stop()
