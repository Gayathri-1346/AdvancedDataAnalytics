import findspark
findspark.init()

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("PySparkSQLJoins") \
    .getOrCreate()

# Employee DataFrame
emp_data = [
    (101, "Ravi"),
    (102, "Priya"),
    (103, "Kiran"),
    (104, "Anita")
]

emp_df = spark.createDataFrame(
    emp_data,
    ["EmpID", "Name"]
)

# Department DataFrame
dept_data = [
    (101, "HR"),
    (102, "Finance"),
    (105, "IT")
]

dept_df = spark.createDataFrame(
    dept_data,
    ["EmpID", "Department"]
)

# Register SQL Views
emp_df.createOrReplaceTempView("employees")
dept_df.createOrReplaceTempView("departments")

print("Employees Table")
emp_df.show()

print("Departments Table")
dept_df.show()


print("INNER JOIN")

spark.sql("""
SELECT e.EmpID, e.Name, d.Department
FROM employees e
INNER JOIN departments d
ON e.EmpID = d.EmpID
""").show()


print("OUTER JOIN")

spark.sql("""
SELECT e.EmpID, e.Name, d.Department
FROM employees e
FULL OUTER JOIN departments d
ON e.EmpID = d.EmpID
""").show()


print("LEFT SEMI JOIN")

spark.sql("""
SELECT *
FROM employees
LEFT SEMI JOIN departments
ON employees.EmpID = departments.EmpID
""").show()


print("CROSS JOIN")

spark.sql("""
SELECT *
FROM employees
CROSS JOIN departments
""").show()

spark.stop()