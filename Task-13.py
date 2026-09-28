import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import NaiveBayes

# Create Spark Session
spark = SparkSession.builder \
    .appName("HospitalNaiveBayes") \
    .getOrCreate()

# Hospital Dataset
# Cold: 1 = Yes, 0 = No
# Flu: 1 = Yes, 0 = No
# Fever: 1 = Yes, 0 = No

data = [
    (1.0, 1.0, 1.0),
    (1.0, 0.0, 1.0),
    (0.0, 1.0, 1.0),
    (0.0, 0.0, 0.0),
    (1.0, 1.0, 1.0),
    (1.0, 0.0, 1.0),
    (0.0, 1.0, 1.0),
    (0.0, 0.0, 0.0)
]

columns = ["Cold", "Flu", "Fever"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Display dataset
df.show()

# Combine Cold and Flu into a single feature vector
assembler = VectorAssembler(
    inputCols=["Cold", "Flu"],
    outputCol="features"
)

df = assembler.transform(df)

# Display features
df.select("Cold", "Flu", "Fever", "features").show()

# Create Naive Bayes model
nb = NaiveBayes(
    featuresCol="features",
    labelCol="Fever"
)

# Train the model
model = nb.fit(df)

# New patient data
# Cold = Yes, Flu = No
new_patient = [
    (1.0, 0.0)
]

new_df = spark.createDataFrame(
    new_patient,
    ["Cold", "Flu"]
)

# Convert input data into feature vector
new_df = assembler.transform(new_df)

# Make prediction
prediction = model.transform(new_df)

# Display prediction
prediction.select(
    "Cold",
    "Flu",
    "prediction",
    "probability"
).show(truncate=False)

# Stop Spark
spark.stop()