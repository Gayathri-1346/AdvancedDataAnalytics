import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, PCA

# Create Spark Session
spark = SparkSession.builder \
    .appName("PCAExample") \
    .getOrCreate()

# Sample Dataset
data = [
    (1.0, 2.0, 3.0),
    (2.0, 3.0, 4.0),
    (3.0, 4.0, 5.0),
    (4.0, 5.0, 6.0),
    (5.0, 6.0, 7.0)
]

# Create DataFrame
df = spark.createDataFrame(
    data,
    ["Feature1", "Feature2", "Feature3"]
)

print("Original Data:")
df.show()

# Convert Features into Vector Format
assembler = VectorAssembler(
    inputCols=["Feature1", "Feature2", "Feature3"],
    outputCol="features"
)

vector_df = assembler.transform(df)

# Apply PCA
pca = PCA(
    k=2,                    # Number of Principal Components
    inputCol="features",
    outputCol="pcaFeatures"
)

# Train PCA Model
pca_model = pca.fit(vector_df)

# Transform Data
result = pca_model.transform(vector_df)

print("PCA Transformed Data:")
result.select(
    "features",
    "pcaFeatures"
).show(truncate=False)

# Display Principal Components
print("Principal Components:")
print(pca_model.pc)

spark.stop()