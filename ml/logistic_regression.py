from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, log1p
# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Logistic_Regression_Model") \
    .master("local[*]") \
    .getOrCreate()

# 2. Load Prepared Dataset
df = spark.read.parquet(
    "data/processed/logistic_customer_data"
)

print("\n===== LOADED LOGISTIC DATASET =====")

df.show(5)

print("Total Customers:", df.count())

# 3. Split Dataset into Training and Testing
# train_df, test_df = df.randomSplit(
#     [0.8, 0.2],
#     seed=42
# )

# Split into training pool and testing data
train_pool, test_df = df.randomSplit(
    [0.8, 0.2],
    seed=42
)

# Divide training pool into training and validation
train_df, validation_df = train_pool.randomSplit(
    [0.875, 0.125],
    seed=42
)

print("\n===== NEW DATASET SPLIT =====")

print("Training:", train_df.count())
print("Validation:", validation_df.count())
print("Testing:", test_df.count())

print("\n===== TRAIN TEST SPLIT =====")

# 4. Transform Monetary and Create Feature Vector

train_df = train_df.withColumn(
    "LogMonetary",
    log1p(col("Monetary"))
)

validation_df = validation_df.withColumn(
    "LogMonetary",
    log1p(col("Monetary"))
)

test_df = test_df.withColumn(
    "LogMonetary",
    log1p(col("Monetary"))
)

assembler = VectorAssembler(
    inputCols=["Recency", "Frequency", "LogMonetary"],
    outputCol="Features"
)

train_df = assembler.transform(train_df)
validation_df = assembler.transform(validation_df)
test_df = assembler.transform(test_df)

# 5. Standardize Features

scaler = StandardScaler(
    inputCol="Features",
    outputCol="ScaledFeatures",
    withStd=True,
    withMean=True
)

# Fit scaler ONLY on training data
scaler_model = scaler.fit(train_df)

train_df = scaler_model.transform(train_df)
validation_df = scaler_model.transform(validation_df)
test_df = scaler_model.transform(test_df)

print("\n===== SCALED FEATURES =====")

train_df.select(
    "Recency",
    "Frequency",
    "Monetary",
    "ScaledFeatures",
    "Label"
).show(5, truncate=False)

# 6. Combine Training and Validation Data

final_train_df = train_df.unionByName(validation_df)

print("\nFinal Training Customers:", final_train_df.count())

# 7. Train Final Logistic Regression Model

lr = LogisticRegression(
    featuresCol="ScaledFeatures",
    labelCol="Label",
    maxIter=100,
    regParam=0.01
)

lr_model = lr.fit(final_train_df)

print("\n===== FINAL LOGISTIC REGRESSION TRAINED =====")

print("Coefficients:", lr_model.coefficients)
print("Intercept:", lr_model.intercept)

# 7. Generate Predictions

predictions = lr_model.transform(test_df)
print("\n===== CUSTOMER PURCHASE PREDICTIONS =====")

predictions.select(
    "CustomerID",
    "Label",
    "prediction",
    "probability"
).show(15, truncate=False)

# 8. Evaluate Logistic Regression Model

evaluator = MulticlassClassificationEvaluator(
    labelCol="Label",
    predictionCol="prediction"
)

accuracy = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "accuracy"}
)

precision = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "weightedPrecision"}
)

recall = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "weightedRecall"}
)

f1 = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "f1"}
)

print("\n===== LOGISTIC REGRESSION EVALUATION =====")

print(f"Accuracy  : {accuracy:.4f}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")
print(f"F1 Score  : {f1:.4f}")

# 9. Confusion Matrix

print("\n===== CONFUSION MATRIX =====")

confusion_matrix = predictions.groupBy(
    "Label", "prediction"
).count().orderBy(
    "Label", "prediction"
)

confusion_matrix.show()

# 4. Check Label Distribution
print("\n===== TRAINING LABEL DISTRIBUTION =====")
train_df.groupBy("Label").count().show()

print("\n===== TESTING LABEL DISTRIBUTION =====")
test_df.groupBy("Label").count().show()


# Save final Logistic Regression model
lr_model.write().overwrite().save(
    "models/logistic_regression_model"
)

scaler_model.write().overwrite().save(
    "models/logistic_scaler"
)

print("\n===== FINAL LOGISTIC MODEL SAVED SUCCESSFULLY =====")


spark.stop()
