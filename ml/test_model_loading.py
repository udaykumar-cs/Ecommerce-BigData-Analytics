from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import StandardScalerModel

# Create Spark Session
spark = SparkSession.builder \
    .appName("Test_Model_Loading") \
    .master("local[*]") \
    .getOrCreate()

# Load saved models
scaler_model = StandardScalerModel.load("models/rfm_scaler")

kmeans_model = KMeansModel.load("models/kmeans_model")

print("\n===== MODELS LOADED SUCCESSFULLY =====")

# Display K-Means cluster centers
print("\n===== LOADED K-MEANS CENTROIDS =====")

for center in kmeans_model.clusterCenters():
    print(center)

# Stop Spark
spark.stop()