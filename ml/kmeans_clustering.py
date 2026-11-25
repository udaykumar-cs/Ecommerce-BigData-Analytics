from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt

def train_kmeans(rfm_scaled_df, k=3):

    # Create K-Means model
    kmeans = KMeans(
        k=k,
        featuresCol="ScaledFeatures",
        predictionCol="Cluster",
        seed=42
    )

    # Train the model
    model = kmeans.fit(rfm_scaled_df)

    # Assign customers to clusters
    predictions = model.transform(rfm_scaled_df)

    return model, predictions
def evaluate_kmeans(predictions):

    evaluator = ClusteringEvaluator(
        featuresCol="ScaledFeatures",
        predictionCol="Cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )

    silhouette_score = evaluator.evaluate(predictions)

    return silhouette_score

def find_optimal_k(rfm_scaled_df, k_values=range(2, 7)):

    results = []

    for k in k_values:

        kmeans = KMeans(
            k=k,
            featuresCol="ScaledFeatures",
            predictionCol="Cluster",
            seed=42
        )

        model = kmeans.fit(rfm_scaled_df)

        # Calculate WCSS
        wcss = model.summary.trainingCost

        # Generate cluster predictions
        predictions = model.transform(rfm_scaled_df)

        # Calculate Silhouette Score
        silhouette = evaluate_kmeans(predictions)

        results.append((k, wcss, silhouette))

        print(
            f"K={k}, WCSS={wcss:.2f}, "
            f"Silhouette={silhouette:.4f}"
        )

    return results

def plot_kmeans_metrics(results):

    k_values = [row[0] for row in results]
    wcss_values = [row[1] for row in results]
    silhouette_values = [row[2] for row in results]

    # Elbow Method graph
    plt.figure(figsize=(8, 5))
    plt.plot(k_values, wcss_values, marker="o")
    plt.xlabel("Number of Clusters (K)")
    plt.ylabel("WCSS")
    plt.title("Elbow Method")
    plt.xticks(k_values)
    plt.grid(True)
    plt.show()

    # Silhouette Score graph
    plt.figure(figsize=(8, 5))
    plt.plot(k_values, silhouette_values, marker="o")
    plt.xlabel("Number of Clusters (K)")
    plt.ylabel("Silhouette Score")
    plt.title("Silhouette Score Analysis")
    plt.xticks(k_values)
    plt.grid(True)
    plt.show()