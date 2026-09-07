import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create Spark Session
spark = SparkSession.builder \
    .appName("HousePricePrediction") \
    .getOrCreate()


# Sample House Data
# (Area, Bedrooms, Bathrooms, Price)

data = [
    (1000.0, 2.0, 1.0, 300000.0),
    (1200.0, 2.0, 2.0, 350000.0),
    (1500.0, 3.0, 2.0, 450000.0),
    (1800.0, 3.0, 2.0, 500000.0),
    (2000.0, 4.0, 3.0, 600000.0),
    (2500.0, 4.0, 3.0, 750000.0),
    (3000.0, 5.0, 4.0, 900000.0)
]


# Create DataFrame
df = spark.createDataFrame(
    data,
    ["Area", "Bedrooms", "Bathrooms", "Price"]
)

print("House Input Data:")
df.show()


# Convert multiple input columns into a feature vector
assembler = VectorAssembler(
    inputCols=["Area", "Bedrooms", "Bathrooms"],
    outputCol="features"
)

data_prepared = assembler.transform(df)


# Select features and target column
final_data = data_prepared.select(
    "features",
    "Price"
)

print("Prepared Data:")
final_data.show()


# Create Linear Regression Model
lr = LinearRegression(
    featuresCol="features",
    labelCol="Price"
)


# Train the Model
model = lr.fit(final_data)


# Predict House Prices
predictions = model.transform(final_data)

print("House Price Predictions:")

predictions.select(
    "features",
    "Price",
    "prediction"
).show()


# Model Parameters
print("Coefficients:", model.coefficients)
print("Intercept:", model.intercept)


# Predict Price for a New House

new_house = [
    (2200.0, 4.0, 3.0)
]

new_house_df = spark.createDataFrame(
    new_house,
    ["Area", "Bedrooms", "Bathrooms"]
)

new_house_prepared = assembler.transform(new_house_df)

new_prediction = model.transform(new_house_prepared)

print("New House Price Prediction:")

new_prediction.select(
    "Area",
    "Bedrooms",
    "Bathrooms",
    "prediction"
).show()


# Stop Spark Session
spark.stop()