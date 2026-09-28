import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

# Create Spark Session
spark = SparkSession.builder \
    .appName("RandomForestClassification") \
    .getOrCreate()


file_path = r"D:\ADA7219\global_superstore.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

num_cols = ["Sales", "Quantity", "Discount", "Shipping Cost", "Profit"]
for c in num_cols:
    df = df.withColumn(c, expr(f"try_cast(regexp_replace(`{c}`, '[$, ]', '') AS DOUBLE)"))


df = df.na.drop(subset=num_cols) \
       .withColumn("Label", when(col("Profit") > 0, 1.0).otherwise(0.0))

print("Input Data Sample:")
df.select("Sales", "Quantity", "Discount", "Shipping Cost", "Label").show(5)


assembler = VectorAssembler(
    inputCols=["Sales", "Quantity", "Discount", "Shipping Cost"],
    outputCol="features"
)

dataset = assembler.transform(df)

final_data = dataset.select("features", "Label")


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
).show(10)

# Display Model Information
print("Number of Trees:", model.getNumTrees)

spark.stop()