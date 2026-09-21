import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
spark = SparkSession.builder \
    .appName("DecisionTreeClassification") \
    .getOrCreate()
data = [
    (2.0, 60.0, 0.0),
    (3.0, 65.0, 0.0),
    (4.0, 70.0, 0.0),
    (5.0, 75.0, 1.0),
    (6.0, 80.0, 1.0),
    (7.0, 90.0, 1.0)
]
df = spark.createDataFrame(
    data,
    ["Hours", "Attendance", "Label"]
)

print("Input Data:")
df.show()
assembler = VectorAssembler(
    inputCols=["Hours", "Attendance"],
    outputCol="features"
)

dataset = assembler.transform(df)
final_data = dataset.select("features", "Label")
dt = DecisionTreeClassifier(
    featuresCol="features",
    labelCol="Label"
)
model = dt.fit(final_data)
predictions = model.transform(final_data)
print("Predictions:")
predictions.select(
    "features",
    "Label",
    "prediction"
).show()
print("Decision Tree Model:")
print(model.toDebugString)
spark.stop()

