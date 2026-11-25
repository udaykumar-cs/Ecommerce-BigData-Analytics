import pandas as pd

comparison_data = [
    {
        "Algorithm": "Logistic Regression",
        "Type": "Supervised - Classification",
        "Accuracy": 0.6667,
        "Precision": 0.6726,
        "Recall": 0.6667,
        "F1": 0.6609,
        "RMSE": None,
        "MAE": None,
        "R2": None,
        "Silhouette": None
    },
    {
        "Algorithm": "Linear Regression",
        "Type": "Supervised - Regression",
        "Accuracy": None,
        "Precision": None,
        "Recall": None,
        "F1": None,
        "RMSE": 1204.5825,
        "MAE": 487.8073,
        "R2": 0.5923,
        "Silhouette": None
    },
    {
        "Algorithm": "K-Means",
        "Type": "Unsupervised - Clustering",
        "Accuracy": None,
        "Precision": None,
        "Recall": None,
        "F1": None,
        "RMSE": None,
        "MAE": None,
        "R2": None,
        "Silhouette": 0.7133
    },
    {
        "Algorithm": "Hierarchical Clustering",
        "Type": "Unsupervised - Clustering",
        "Accuracy": None,
        "Precision": None,
        "Recall": None,
        "F1": None,
        "RMSE": None,
        "MAE": None,
        "R2": None,
        "Silhouette": 0.7138
    }
]

comparison_df = pd.DataFrame(comparison_data)

print("\n===== MACHINE LEARNING MODEL COMPARISON =====")
print(comparison_df.to_string(index=False))

comparison_df.to_csv(
    "results/model_comparison.csv",
    index=False
)

print("\nModel comparison report saved successfully.")