from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import StandardScalerModel

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Test_Logistic_Model_Loading") \
    .master("local[*]") \
    .getOrCreate()

# 2. Load saved models
lr_model = LogisticRegressionModel.load(
    "models/logistic_regression_model"
)

scaler_model = StandardScalerModel.load(
    "models/logistic_scaler"
)

# 3. Verify loading
print("\n===== LOGISTIC MODELS LOADED SUCCESSFULLY =====")

print("Coefficients:", lr_model.coefficients)
print("Intercept:", lr_model.intercept)

print("\nScaler loaded successfully!")

# 4. Stop Spark
spark.stop()