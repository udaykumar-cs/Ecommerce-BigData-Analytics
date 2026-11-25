from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    max,
    countDistinct,
    datediff,
    lit
)

from pyspark.ml.feature import VectorAssembler, StandardScaler

def create_rfm_features(sales_df, snapshot_date="2011-12-10"):

    # Exclude transactions without CustomerID
    customer_df = sales_df.filter(
        col("CustomerID").isNotNull()
    )

    # Calculate RFM metrics
    rfm_df = customer_df.groupBy("CustomerID").agg(

        max("InvoiceDate").alias("Last Purchase Date"),

        countDistinct("InvoiceNo").alias("Frequency"),

        spark_sum("Revenue").alias("Monetary")
    )

    # Calculate Recency
    rfm_df = rfm_df.withColumn(
        "Recency",
        datediff(
            lit(snapshot_date),
            col("Last Purchase Date")
        )
    )

    return rfm_df

def prepare_scaled_features(rfm_df):

    # Combine RFM columns into a single feature vector
    assembler = VectorAssembler(
        inputCols=["Recency", "Frequency", "Monetary"],
        outputCol="Features"
    )

    rfm_vector_df = assembler.transform(rfm_df)

    # Standardize the feature vector
    scaler = StandardScaler(
        inputCol="Features",
        outputCol="ScaledFeatures",
        withStd=True,
        withMean=True
    )

    scaler_model = scaler.fit(rfm_vector_df)

    rfm_scaled_df = scaler_model.transform(rfm_vector_df)

    return rfm_scaled_df, scaler_model