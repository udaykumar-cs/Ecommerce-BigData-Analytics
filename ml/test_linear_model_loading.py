from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import StandardScalerModel

spark = SparkSession.builder \
    .appName("Test_Linear_Model_Loading") \
    .master("local[*]") \
    .getOrCreate()

lr_model = LinearRegressionModel.load(
    "models/linear_regression_model"
)

scaler_model = StandardScalerModel.load(
    "models/linear_scaler"
)

print("\n===== LINEAR REGRESSION MODELS LOADED SUCCESSFULLY =====")

print("Coefficients:", lr_model.coefficients)
print("Intercept:", lr_model.intercept)

spark.stop()