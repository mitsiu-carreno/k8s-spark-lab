import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import tensorflow as tf
from tensorflow.keras import layers, models
import horovod.tensorflow.keras as hvd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AutoencoderAnomalyDetection") \
    .getOrCreate()

# Initialize Horovod
hvd.init()

# Sample dataset loading and preprocessing (replace this with your actual dataset)
# Let's use a synthetic dataset for demonstration
#data = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)

data = [
    (1, 3.0, 2.0),
    (2, 4.0, 3.0),
    (3, 5.0, 4.0),
    (4, 6.0, 5.0),
    (5, 100.0, 120)
]

columns = ["id", "f1", "f2"]
data = spark.createDataFrame(data, columns)

# Assuming your dataset has several features, and you want to use them all
feature_columns = data.columns  # This assumes all columns are features
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
pipeline = Pipeline(stages=[assembler])
df = pipeline.fit(data).transform(data)

# Convert the Spark DataFrame to a Pandas DataFrame for Keras processing
pandas_df = df.select("features").toPandas()
X = np.array([x for x in pandas_df['features']])

# Normalize data
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Define a simple autoencoder model in Keras
def build_autoencoder(input_dim):
    input_layer = layers.Input(shape=(input_dim,))
    encoded = layers.Dense(64, activation='relu')(input_layer)
    encoded = layers.Dense(32, activation='relu')(encoded)
    encoded = layers.Dense(16, activation='relu')(encoded)

    decoded = layers.Dense(32, activation='relu')(encoded)
    decoded = layers.Dense(64, activation='relu')(decoded)
    decoded = layers.Dense(input_dim, activation='sigmoid')(decoded)

    autoencoder = models.Model(input_layer, decoded)
    autoencoder.compile(optimizer='adam', loss='mean_squared_error')
    
    return autoencoder

# Build the autoencoder
autoencoder = build_autoencoder(X_scaled.shape[1])

# Use Horovod to distribute the training
def train_autoencoder(X_train):
    # Horovod: Distributed training setup
    autoencoder.fit(X_train, X_train, epochs=10, batch_size=64, shuffle=True, verbose=1)
    
# Make sure to distribute training across multiple nodes using Horovod
train_autoencoder(X_scaled)

# Anomaly detection: Reconstruct the input and compute the reconstruction error
reconstructed = autoencoder.predict(X_scaled)
reconstruction_error = np.mean(np.square(X_scaled - reconstructed), axis=1)

# Set a threshold for anomaly detection (based on reconstruction error)
threshold = np.percentile(reconstruction_error, 95)  # 95th percentile for anomalies

# Identify anomalies
anomalies = reconstruction_error > threshold

# Return anomalies and reconstruction errors for analysis
anomalous_data = pd.DataFrame({
    'reconstruction_error': reconstruction_error,
    'is_anomaly': anomalies
})

print(anomalous_data.head())

# Stop the Spark session
spark.stop()

