import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName("autoencoder").getOrCreate()

# Generate or load dataset (using synthetic data here for illustration)
num_samples = 100000
num_features = 20
data = np.random.rand(num_samples, num_features)

# Create Spark DataFrame
columns = [f"feature_{i}" for i in range(num_features)]
df = spark.createDataFrame(data.tolist(), columns)

# Convert features to a single vector column for scaling
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=columns, outputCol="features")
df_vector = assembler.transform(df)

# Normalize the data using StandardScaler (distributed processing)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(df_vector)
df_scaled = scaler_model.transform(df_vector)

# Preprocess the scaled data and collect it into a NumPy array for model training
def preprocess_data(df):
    data = np.array([row['scaled_features'].toArray() for row in df.collect()])
    return data

train_data = preprocess_data(df_scaled)

# Define the Autoencoder model for anomaly detection
input_dim = train_data.shape[1]  # number of features in the data
encoding_dim = 10  # number of nodes in the encoding layer

input_layer = keras.Input(shape=(input_dim,))
encoded = layers.Dense(encoding_dim, activation='relu')(input_layer)
decoded = layers.Dense(input_dim, activation='sigmoid')(encoded)

autoencoder = keras.Model(input_layer, decoded)

# Compile the model
autoencoder.compile(optimizer='adam', loss='mean_squared_error')

# Train the autoencoder (using the normal data)
autoencoder.fit(train_data, train_data, epochs=10, batch_size=256, shuffle=True)

# Get the model predictions (reconstructed data)
reconstructed_data = autoencoder.predict(train_data)

# Calculate reconstruction error (mean squared error between original and reconstructed data)
reconstruction_error = np.mean(np.square(train_data - reconstructed_data), axis=1)

# Set an anomaly threshold (based on reconstruction error)
threshold = np.percentile(reconstruction_error, 95)  # e.g., top 5% as anomalies

# Flag anomalies based on reconstruction error exceeding the threshold
anomalies = reconstruction_error > threshold

# Show the results of anomaly detection
print(f"Anomaly threshold: {threshold}")
print(f"Number of anomalies detected: {np.sum(anomalies)}")

"""
# Optional: Visualize the reconstruction error and threshold
plt.figure(figsize=(10, 6))
plt.hist(reconstruction_error, bins=50, alpha=0.75)
plt.axvline(threshold, color='r', linestyle='--', label='Threshold')
plt.title("Reconstruction Error Distribution for Anomaly Detection")
plt.xlabel("Reconstruction Error")
plt.ylabel("Frequency")
plt.legend()
plt.show()
"""

# Convert the anomalies back to a Spark DataFrame
anomalies_df = spark.createDataFrame([(i, anomalies[i]) for i in range(len(anomalies))], ["index", "is_anomaly"])
anomalies_df.show(10)
