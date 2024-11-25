from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline

import numpy as np

import tensorflow as tf
import tensorflowonspark as tfos

spark = SparkSession.builder.appName("autoencoder").getOrCreate()

df = spark.read.parquet("s3a://logs/output/test/domain=2021.designa.mx/")

feature_columns = ['fabstime', 'body_bytes_sent']

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")

pipeline = Pipeline(stages=[assembler, scaler])

df_scaled = pipeline.fit(df).transform(df)

df_scaled.show(5)

# Function to extract data from Spark DataFrame in batches
def get_data_batch(start_idx, end_idx, df_scaled):
    # Extract the scaled features for the current batch
    batch_df = df_scaled.select("scaled_features").rdd.zipWithIndex().filter(lambda x: start_idx <= x[1] < end_idx).map(lambda x: x[0][0]).toPandas()
    
    # Convert the pandas DataFrame to a numpy array
    batch_data = np.array([x for x in batch_df['scaled_features']])
    return batch_data


def build_autoencoder(input_dim):
    # Define a simple autoencoder model
    input_layer = tf.keras.layers.Input(shape=(input_dim,))
    encoded = tf.keras.layers.Dense(64, activation='relu')(input_layer)
    encoded = tf.keras.layers.Dense(32, activation='relu')(encoded)
    bottleneck = tf.keras.layers.Dense(16, activation='relu')(encoded)

    decoded = tf.keras.layers.Dense(32, activation='relu')(bottleneck)
    decoded = tf.keras.layers.Dense(64, activation='relu')(decoded)
    output_layer = tf.keras.layers.Dense(input_dim, activation='sigmoid')(decoded)

    # Build the model
    autoencoder = tf.keras.models.Model(input_layer, output_layer)
    autoencoder.compile(optimizer='adam', loss='mse')  # Mean Squared Error for reconstruction
    return autoencoder


# The map_fn will be executed on each worker node
def map_fn(args, ctx):
    from tensorflow import keras
    import tensorflow as tf
    import numpy as np
    
    # Load your batch data (start_idx and end_idx will be provided by TensorFlowOnSpark)
    X_batch = get_data_batch(args['start_idx'], args['end_idx'], df_scaled)

    # Define the autoencoder model
    input_dim = X_batch.shape[1]  # number of features
    autoencoder = build_autoencoder(input_dim)

    # Train the model on the batch data
    autoencoder.fit(X_batch, X_batch, epochs=1, batch_size=1024)

    # Save the model (this model will be sent back to Spark workers)
    autoencoder.save(args['model_dir'])


# Define configuration for TensorFlowOnSpark
conf = {
    "spark.executorEnv.TF_CUDA": "1",  # Enable GPU if available (optional)
    "spark.submit.deployMode": "client",  # or "cluster"
}

# Initialize distributed training with TensorFlowOnSpark
num_workers = 4  # Adjust according to your cluster size
batch_size = 1024

# Parameters for distributed training
args = {
    'start_idx': 0,
    'end_idx': batch_size,
    'model_dir': 'model_checkpoint_dir'
}

# Running the distributed training job
tfos.TFJobRunner.run(conf, map_fn, args)


def detect_anomalies(model, data):
    # Reconstruct the data
    reconstructed = model.predict(data)
    
    # Calculate reconstruction error (Mean Squared Error)
    reconstruction_error = np.mean(np.square(data - reconstructed), axis=1)
    
    # Define an anomaly threshold (this threshold can be adjusted)
    threshold = 0.1
    
    # Flag anomalies
    anomalies = reconstruction_error > threshold
    return anomalies

# Example usage for detecting anomalies
model = tf.keras.models.load_model("model_checkpoint_dir")  # Load the trained model
X_data = get_data_batch(0, 1000, df_scaled)  # Fetch a batch of data

anomalies = detect_anomalies(model, X_data)
print("Anomalies detected:", anomalies)

