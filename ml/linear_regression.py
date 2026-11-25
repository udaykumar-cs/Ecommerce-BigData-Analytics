from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator


# Create Spark Session
spark = SparkSession.builder \
    .appName("Linear_Regression_Model") \
    .master("local[*]") \
    .getOrCreate()


print("\n===== SPARK SESSION CREATED =====")

# Load prepared customer dataset
df = spark.read.parquet(
    "data/processed/linear_customer_data"
)

print("\n===== LINEAR REGRESSION DATASET LOADED =====")

print("Total Customers:", df.count())

df.printSchema()

df.show(5)

# Select required columns
df = df.select(
    "Recency",
    "Frequency",
    "Monetary",
    "FutureSpend"
)

print("\n===== SELECTED FEATURES =====")
df.show(5)


# Split dataset into training and testing
train_df, test_df = df.randomSplit(
    [0.8, 0.2],
    seed=42
)

print("\n===== TRAIN TEST SPLIT =====")
print("Total Records:", df.count())
print("Training Records:", train_df.count())
print("Testing Records:", test_df.count())


# Assemble input features
assembler = VectorAssembler(
    inputCols=["Recency", "Frequency", "Monetary"],
    outputCol="Features"
)

train_df = assembler.transform(train_df)
test_df = assembler.transform(test_df)


# Create StandardScaler
scaler = StandardScaler(
    inputCol="Features",
    outputCol="ScaledFeatures",
    withStd=True,
    withMean=True
)

# Fit scaler only on training data
scaler_model = scaler.fit(train_df)

# Transform training and testing data
train_df = scaler_model.transform(train_df)
test_df = scaler_model.transform(test_df)


print("\n===== SCALED FEATURES =====")

train_df.select(
    "Features",
    "ScaledFeatures"
).show(5, truncate=False)

# Create Linear Regression model
lr = LinearRegression(
    featuresCol="ScaledFeatures",
    labelCol="FutureSpend",
    maxIter=100,
    regParam=0.01
)

# Train the model
lr_model = lr.fit(train_df)

print("\n===== LINEAR REGRESSION MODEL TRAINED =====")

print("Coefficients:", lr_model.coefficients)
print("Intercept:", lr_model.intercept)

# Generate predictions
predictions = lr_model.transform(test_df)

print("\n===== LINEAR REGRESSION PREDICTIONS =====")

predictions.select(
    "FutureSpend",
    "prediction"
).show(10, truncate=False)

# Evaluate Linear Regression Model

rmse_evaluator = RegressionEvaluator(
    labelCol="FutureSpend",
    predictionCol="prediction",
    metricName="rmse"
)

mae_evaluator = RegressionEvaluator(
    labelCol="FutureSpend",
    predictionCol="prediction",
    metricName="mae"
)

r2_evaluator = RegressionEvaluator(
    labelCol="FutureSpend",
    predictionCol="prediction",
    metricName="r2"
)

rmse = rmse_evaluator.evaluate(predictions)
mae = mae_evaluator.evaluate(predictions)
r2 = r2_evaluator.evaluate(predictions)

print("\n===== LINEAR REGRESSION EVALUATION =====")
print("RMSE:", rmse)
print("MAE :", mae)
print("R2  :", r2)

# Save Linear Regression Model

lr_model.write().overwrite().save(
    "models/linear_regression_model"
)

scaler_model.write().overwrite().save(
    "models/linear_scaler"
)

print("\n===== LINEAR REGRESSION MODELS SAVED SUCCESSFULLY =====")