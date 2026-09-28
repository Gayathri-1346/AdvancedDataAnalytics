import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

# Create Spark Session
spark = SparkSession.builder \
    .appName("RandomForestClassification") \
    .getOrCreate()

# Sample Dataset
# Hours_Studied, Attendance, Result
data = [
    (2.0, 60.0, 0.0),
    (3.0, 65.0, 0.0),
    (4.0, 70.0, 0.0),
    (5.0, 75.0, 1.0),
    (6.0, 80.0, 1.0),
    (7.0, 90.0, 1.0)
]

# Create DataFrame
df = spark.createDataFrame(
    data,
    ["Hours", "Attendance", "Label"]
)

print("Input Data:")
df.show()

# Convert Features into Vector Format
assembler = VectorAssembler(
    inputCols=["Hours", "Attendance"],
    outputCol="features"
)

dataset = assembler.transform(df)

# Select Features and Label
final_data = dataset.select("features", "Label")

# Create Random Forest Classifier
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="Label",
    numTrees=10
)

# Train Model
model = rf.fit(final_data)

# Predictions
predictions = model.transform(final_data)

print("Predictions:")
predictions.select(
    "features",
    "Label",
    "prediction"
).show()

# Display Model Information
print("Number of Trees:", model.getNumTrees)

spark.stop()

